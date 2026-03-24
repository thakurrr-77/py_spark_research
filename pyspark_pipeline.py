"""
pyspark_pipeline.py
--------------------
Production-ready ETL + Analytics pipeline using PySpark (distributed baseline).

Pipeline stages mirror pandas_pipeline.py exactly so results are comparable:
  1. EXTRACT  – Read CSV via Spark
  2. TRANSFORM – Clean, deduplicate, enrich, feature-engineer (Spark SQL + DataFrame API)
  3. LOAD      – Write cleaned Parquet (snappy)
  4. ANALYTICS – Aggregate KPIs with Spark actions

Metrics captured (same schema as pandas_pipeline.py):
  - Wall-clock time per stage + total
  - Peak driver RSS memory (psutil)
  - SparkUI JVM metrics (executor memory via REST API when available)
  - Output row count / null rate before & after
  - Cores used = Spark local[*] (all available cores)

Authors : Aayush Ranjan & Shubham Thakur
Institute: Chitkara Institute of Engineering and Technology
"""

from __future__ import annotations

import gc
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

# ── Windows: auto-set HADOOP_HOME so PySpark can write files ─────────────────
if sys.platform == "win32":
    _hadoop_home = Path("C:/hadoop")
    _hadoop_bin  = _hadoop_home / "bin"
    if _hadoop_bin.exists():
        os.environ.setdefault("HADOOP_HOME", str(_hadoop_home))
        _path = os.environ.get("PATH", "")
        if str(_hadoop_bin) not in _path:
            os.environ["PATH"] = str(_hadoop_bin) + os.pathsep + _path

import psutil
from pyspark.sql import SparkSession, DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, FloatType, StringType, TimestampType, BooleanType,
)
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR  = Path("output/pyspark")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRICS_DIR = Path("metrics")
METRICS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def _build_spark(app_name: str = "ETL_Pipeline") -> SparkSession:
    """
    Build a local SparkSession tuned for a development / benchmarking workload.
    Uses all available CPU cores; driver memory is set to 4 GB.
    """
    cores = os.cpu_count() or 2
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory",         "4g")
        .config("spark.sql.shuffle.partitions", str(max(cores * 2, 4)))
        .config("spark.sql.adaptive.enabled",   "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled",             "false")   # disable UI in bench
        .config("spark.eventLog.enabled",       "false")
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.rootCategory=WARN,console")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession started  master=%s  cores=%d  version=%s",
        spark.sparkContext.master, cores, spark.version,
    )
    return spark


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType([
    StructField("transaction_id",    LongType(),      True),
    StructField("customer_id",       LongType(),      True),
    StructField("product_id",        LongType(),      True),
    StructField("product_name",      StringType(),    True),
    StructField("category",          StringType(),    True),
    StructField("price",             FloatType(),     True),
    StructField("quantity",          IntegerType(),   True),
    StructField("discount",          FloatType(),     True),
    StructField("payment_method",    StringType(),    True),
    StructField("status",            StringType(),    True),
    StructField("region",            StringType(),    True),
    StructField("device_type",       StringType(),    True),
    StructField("timestamp",         StringType(),    True),   # cast in transform
    StructField("is_duplicate_flag", BooleanType(),   True),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _memory_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)


def _null_rate_spark(df: SparkDF) -> float:
    """Fraction of null cells across all columns (requires a full scan – O(N))."""
    total_cells = df.count() * len(df.columns)
    if total_cells == 0:
        return 0.0
    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0]
    return sum(null_counts) / total_cells


# ---------------------------------------------------------------------------
# Stage 1 – EXTRACT
# ---------------------------------------------------------------------------

def extract(
    spark: SparkSession,
    csv_path: str | Path,
) -> tuple[SparkDF, float, float]:
    """
    Read CSV into a Spark DataFrame (lazy; action triggered by count).

    Returns
    -------
    df, wall_time_sec, delta_mem_mb
    """
    csv_path   = str(Path(csv_path).resolve())
    mem_before = _memory_mb()
    logger.info("[EXTRACT] Reading: %s", csv_path)
    t0 = time.perf_counter()

    df = (
        spark.read
        .option("header",         "true")
        .option("inferSchema",    "false")
        .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
        .schema(RAW_SCHEMA)
        .csv(csv_path)
    )

    # Trigger an action to measure true read time
    row_count = df.count()
    elapsed   = time.perf_counter() - t0
    mem_used  = _memory_mb() - mem_before

    logger.info(
        "[EXTRACT] Rows=%s  Cols=%d  Time=%.3fs  ΔMemory=%.1fMB",
        f"{row_count:,}", len(df.columns), elapsed, mem_used,
    )
    return df, elapsed, mem_used


# ---------------------------------------------------------------------------
# Stage 2 – TRANSFORM
# ---------------------------------------------------------------------------

def transform(
    df: SparkDF,
) -> tuple[SparkDF, float, float, dict[str, Any]]:
    """
    Distributed ETL transform — mirrors pandas_pipeline.transform exactly.

    Returns
    -------
    df_clean, wall_time_sec, delta_mem_mb, quality_report
    """
    logger.info("[TRANSFORM] Starting …")
    mem_before   = _memory_mb()
    t0           = time.perf_counter()

    rows_before  = df.count()
    nulls_before = _null_rate_spark(df)

    # ── 2.1 Remove exact duplicate rows ───────────────────────────────────────
    df = df.dropDuplicates()

    # ── 2.2 Remove transaction_id duplicates (keep first by timestamp) ────────
    w  = Window.partitionBy("transaction_id").orderBy("timestamp")
    df = (
        df.withColumn("_row_num", F.row_number().over(w))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )

    # ── 2.3 Drop critical-column nulls ────────────────────────────────────────
    critical_cols = ["transaction_id", "customer_id", "price",
                     "quantity", "category", "status", "region"]
    df = df.dropna(subset=critical_cols)

    # ── 2.4 Fill non-critical nulls ───────────────────────────────────────────
    df = df.fillna({"product_name": "Unknown Product", "discount": 0.0})

    # ── 2.5 Validate & clip ───────────────────────────────────────────────────
    df = df.filter((F.col("price") > 0) & (F.col("quantity") > 0))
    df = df.withColumn("discount", F.least(F.greatest(F.col("discount"), F.lit(0.0)), F.lit(1.0)))

    # ── 2.6 Feature engineering ───────────────────────────────────────────────
    # Cast string timestamp — use try_to_timestamp to tolerate sub-second precision
    df = df.withColumn(
        "timestamp",
        F.coalesce(
            F.try_to_timestamp(F.col("timestamp"), F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp(
                F.substring(F.col("timestamp"), 1, 19),
                F.lit("yyyy-MM-dd HH:mm:ss"),
            ),
        ),
    )
    df = (
        df.withColumn("gross_revenue", F.col("price")         * F.col("quantity"))
          .withColumn("discount_amt",  F.col("gross_revenue") * F.col("discount"))
          .withColumn("net_revenue",   F.col("gross_revenue") - F.col("discount_amt"))
          .withColumn("month",         F.month("timestamp"))
          .withColumn("year",          F.year("timestamp"))
          .withColumn("quarter",       F.quarter("timestamp"))
          .withColumn("dow",           F.dayofweek("timestamp"))
    )

    # Revenue band (bucketed)
    df = df.withColumn(
        "revenue_band",
        F.when(F.col("net_revenue") <  50,    "micro")
         .when(F.col("net_revenue") <  200,   "low")
         .when(F.col("net_revenue") <  500,   "medium")
         .when(F.col("net_revenue") <  1_000, "high")
         .otherwise("premium"),
    )

    # High-value flag (90th-percentile threshold)
    p90 = df.approxQuantile("net_revenue", [0.90], 0.001)[0]
    df  = df.withColumn("is_high_value", (F.col("net_revenue") > p90).cast("int"))

    # ── 2.7 Cache cleaned DataFrame for downstream stages ─────────────────────
    df.cache()

    rows_after   = df.count()
    nulls_after  = _null_rate_spark(df)
    quality_rpt  = {
        "rows_before":      rows_before,
        "rows_after":       rows_after,
        "rows_dropped":     rows_before - rows_after,
        "drop_pct":         round((rows_before - rows_after) / max(rows_before, 1) * 100, 2),
        "null_rate_before": round(nulls_before * 100, 4),
        "null_rate_after":  round(nulls_after  * 100, 4),
    }

    elapsed  = time.perf_counter() - t0
    mem_used = _memory_mb() - mem_before
    logger.info(
        "[TRANSFORM] Rows %s→%s  NullRate %.2f%%→%.2f%%  Time=%.3fs  ΔMemory=%.1fMB",
        f"{rows_before:,}", f"{rows_after:,}",
        nulls_before * 100, nulls_after * 100,
        elapsed, mem_used,
    )
    return df, elapsed, mem_used, quality_rpt


# ---------------------------------------------------------------------------
# Stage 3 – LOAD
# ---------------------------------------------------------------------------

def load(df: SparkDF, label: str) -> tuple[float, float]:
    """
    Write cleaned DataFrame to Parquet.

    Returns
    -------
    wall_time_sec, delta_mem_mb
    """
    out_path   = str((OUTPUT_DIR / f"clean_{label}").resolve())
    mem_before = _memory_mb()
    t0         = time.perf_counter()

    logger.info("[LOAD] Writing Parquet → %s", out_path)
    df.write.mode("overwrite").parquet(out_path)

    elapsed  = time.perf_counter() - t0
    mem_used = _memory_mb() - mem_before

    # Approximate size
    out_dir  = Path(out_path)
    size_mb  = sum(f.stat().st_size for f in out_dir.rglob("*.parquet")) / (1024 ** 2)
    logger.info(
        "[LOAD] Written ~%.1fMB  Time=%.3fs  ΔMemory=%.1fMB",
        size_mb, elapsed, mem_used,
    )
    return elapsed, mem_used


# ---------------------------------------------------------------------------
# Stage 4 – ANALYTICS
# ---------------------------------------------------------------------------

def analytics(
    df: SparkDF,
    label: str,
) -> tuple[dict[str, Any], float, float]:
    """
    Compute the same KPIs as pandas_pipeline.analytics using Spark SQL actions.

    Returns
    -------
    kpis, wall_time_sec, delta_mem_mb
    """
    logger.info("[ANALYTICS] Computing KPIs …")
    mem_before = _memory_mb()
    t0         = time.perf_counter()

    kpis: dict[str, Any] = {}

    # ── Overall KPIs ──────────────────────────────────────────────────────────
    overall = df.agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("gross_revenue").alias("total_gross_revenue"),
        F.sum("net_revenue").alias("total_net_revenue"),
        F.sum("discount_amt").alias("total_discount_amt"),
        F.avg("net_revenue").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products"),
        F.avg("is_high_value").alias("high_value_pct"),
    ).collect()[0]

    kpis["total_transactions"]  = int(overall["total_transactions"])
    kpis["total_gross_revenue"] = round(float(overall["total_gross_revenue"] or 0), 2)
    kpis["total_net_revenue"]   = round(float(overall["total_net_revenue"]   or 0), 2)
    kpis["total_discount_amt"]  = round(float(overall["total_discount_amt"]  or 0), 2)
    kpis["avg_order_value"]     = round(float(overall["avg_order_value"]     or 0), 2)
    kpis["unique_customers"]    = int(overall["unique_customers"])
    kpis["unique_products"]     = int(overall["unique_products"])
    kpis["high_value_txn_pct"]  = round(float(overall["high_value_pct"] or 0) * 100, 2)

    # Median (approx)
    kpis["median_order_value"] = round(
        float(df.approxQuantile("net_revenue", [0.50], 0.001)[0]), 2
    )

    # Status rates
    status_counts = (
        df.groupBy("status").count().collect()
    )
    total_txn = kpis["total_transactions"]
    status_map = {r["status"]: r["count"] for r in status_counts}
    kpis["cancellation_rate"] = round(
        status_map.get("cancelled", 0) / max(total_txn, 1) * 100, 2
    )
    kpis["refund_rate"] = round(
        status_map.get("refunded", 0) / max(total_txn, 1) * 100, 2
    )

    # ── Category aggregation ──────────────────────────────────────────────────
    cat_rows = (
        df.groupBy("category")
          .agg(
              F.count("transaction_id").alias("transactions"),
              F.sum("net_revenue").alias("net_revenue"),
              F.avg("discount").alias("avg_discount"),
          )
          .orderBy(F.col("net_revenue").desc())
          .collect()
    )
    kpis["category_revenue"] = {
        r["category"]: {
            "transactions": int(r["transactions"]),
            "net_revenue":  round(float(r["net_revenue"]), 2),
            "avg_discount": round(float(r["avg_discount"] or 0), 4),
        }
        for r in cat_rows
    }

    # ── Monthly trend ─────────────────────────────────────────────────────────
    monthly_rows = (
        df.groupBy("year", "month")
          .agg(F.sum("net_revenue").alias("net_revenue"))
          .orderBy("year", "month")
          .collect()
    )
    kpis["monthly_trend"] = [
        {"year": r["year"], "month": r["month"],
         "net_revenue": round(float(r["net_revenue"]), 2)}
        for r in monthly_rows
    ]

    # ── Payment method ────────────────────────────────────────────────────────
    pay_rows = (
        df.groupBy("payment_method")
          .agg(F.sum("net_revenue").alias("net_revenue"))
          .orderBy(F.col("net_revenue").desc())
          .collect()
    )
    kpis["payment_method_revenue"] = {
        r["payment_method"]: round(float(r["net_revenue"]), 2)
        for r in pay_rows
    }

    # ── Region performance ────────────────────────────────────────────────────
    region_rows = (
        df.groupBy("region")
          .agg(
              F.count("transaction_id").alias("transactions"),
              F.sum("net_revenue").alias("net_revenue"),
          )
          .orderBy(F.col("net_revenue").desc())
          .collect()
    )
    kpis["region_performance"] = {
        r["region"]: {
            "transactions": int(r["transactions"]),
            "net_revenue":  round(float(r["net_revenue"]), 2),
        }
        for r in region_rows
    }

    # ── Revenue band distribution ─────────────────────────────────────────────
    band_rows = (
        df.groupBy("revenue_band")
          .count()
          .collect()
    )
    total = sum(r["count"] for r in band_rows)
    kpis["revenue_band_dist"] = {
        r["revenue_band"]: round(r["count"] / max(total, 1) * 100, 2)
        for r in band_rows
    }

    # ── Top 10 customers ──────────────────────────────────────────────────────
    top_cust_rows = (
        df.groupBy("customer_id")
          .agg(F.sum("net_revenue").alias("net_revenue"))
          .orderBy(F.col("net_revenue").desc())
          .limit(10)
          .collect()
    )
    kpis["top_10_customers"] = {
        str(int(r["customer_id"])): round(float(r["net_revenue"]), 2)
        for r in top_cust_rows
    }

    # Persist analytics JSON
    out_path = METRICS_DIR / f"pyspark_analytics_{label}.json"
    with open(out_path, "w") as f:
        json.dump(kpis, f, indent=2, default=str)
    logger.info("[ANALYTICS] Saved → %s", out_path)

    elapsed  = time.perf_counter() - t0
    mem_used = _memory_mb() - mem_before
    logger.info("[ANALYTICS] Done  Time=%.3fs  ΔMemory=%.1fMB", elapsed, mem_used)
    return kpis, elapsed, mem_used


# ---------------------------------------------------------------------------
# Full pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(
    csv_path:  str | Path,
    label:     str,
    spark:     SparkSession | None = None,
) -> dict[str, Any]:
    """
    Execute the full PySpark ETL pipeline and return a structured metrics dict.

    Parameters
    ----------
    csv_path : str | Path
    label    : str   e.g. '10K', '100K', '1M'
    spark    : SparkSession (optional – created internally if None)

    Returns
    -------
    dict with timing, memory, quality, and KPI information.
    """
    logger.info("=" * 60)
    logger.info("  PYSPARK PIPELINE  |  Scale: %s", label)
    logger.info("=" * 60)

    own_spark   = spark is None
    spark       = spark or _build_spark(f"ETL_{label}")
    cores       = spark.sparkContext.defaultParallelism

    peak_mem_start = _memory_mb()
    total_t0       = time.perf_counter()
    process        = psutil.Process(os.getpid())

    # ── Stage 1: Extract ──────────────────────────────────────────────────────
    df, t_extract, m_extract = extract(spark, csv_path)
    raw_rows = df.count()

    # ── Stage 2: Transform ────────────────────────────────────────────────────
    df, t_transform, m_transform, quality = transform(df)

    # ── Stage 3: Load ─────────────────────────────────────────────────────────
    t_load, m_load = load(df, label)

    # ── Stage 4: Analytics ────────────────────────────────────────────────────
    kpis, t_analytics, m_analytics = analytics(df, label)

    df.unpersist()
    total_time  = time.perf_counter() - total_t0
    peak_rss    = max(process.memory_info().rss / (1024 ** 2), peak_mem_start)

    if own_spark:
        spark.stop()
        logger.info("SparkSession stopped.")

    metrics = {
        "framework":        "pyspark",
        "scale_label":      label,
        "raw_rows":         raw_rows,
        "clean_rows":       quality["rows_after"],
        "cores_used":       cores,
        "time_extract_s":   round(t_extract,   4),
        "time_transform_s": round(t_transform, 4),
        "time_load_s":      round(t_load,      4),
        "time_analytics_s": round(t_analytics, 4),
        "time_total_s":     round(total_time,  4),
        "memory_peak_mb":   round(peak_rss,    2),
        "quality":          quality,
        "kpis":             kpis,
    }

    out = METRICS_DIR / f"pyspark_metrics_{label}.json"
    with open(out, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    logger.info("Metrics saved → %s", out)

    gc.collect()
    return metrics


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from data_generator import generate_dataset, SCALE_CONFIGS

    scales = sys.argv[1:] if len(sys.argv) > 1 else list(SCALE_CONFIGS.keys())
    spark  = _build_spark("ETL_Benchmark")

    for scale in scales:
        cfg  = SCALE_CONFIGS[scale]
        path = generate_dataset(scale)
        run_pipeline(path, cfg["label"], spark=spark)

    spark.stop()
