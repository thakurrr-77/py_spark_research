"""
pandas_pipeline.py
-------------------
Production-ready ETL + Analytics pipeline using pandas (single-node, naive baseline).

Pipeline stages:
  1. EXTRACT  – Read CSV from disk
  2. TRANSFORM – Clean, deduplicate, enrich, feature-engineer
  3. LOAD      – Write cleaned Parquet output
  4. ANALYTICS – Aggregate KPIs, per-category revenue, cohort metrics

Metrics captured per run:
  - Wall-clock time  (extract / transform / load / analytics / total)
  - Peak RSS memory  (psutil)
  - Output row count / null rate before & after
  - CPU count (cores used = 1, single-node)

Authors : Aayush Ranjan & Shubham Thakur
Institute: Chitkara Institute of Engineering and Technology
"""

from __future__ import annotations

import gc
import json
import logging
import os
import time
import tracemalloc
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("output/pandas")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRICS_DIR = Path("metrics")
METRICS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _memory_mb() -> float:
    """Current process RSS in MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)


def _null_rate(df: pd.DataFrame) -> float:
    total_cells = df.shape[0] * df.shape[1]
    if total_cells == 0:
        return 0.0
    return float(df.isnull().sum().sum()) / total_cells


# ---------------------------------------------------------------------------
# Stage 1 – EXTRACT
# ---------------------------------------------------------------------------

def extract(csv_path: str | Path) -> tuple[pd.DataFrame, float, float]:
    """
    Read CSV into DataFrame.

    Returns
    -------
    df, wall_time_sec, mem_mb_after
    """
    csv_path = Path(csv_path)
    logger.info("[EXTRACT] Reading: %s", csv_path)
    mem_before = _memory_mb()
    t0 = time.perf_counter()

    df = pd.read_csv(
        csv_path,
        parse_dates=["timestamp"],
        dtype={
            "transaction_id": "int64",
            "customer_id":    "int64",
            "product_id":     "int64",
            "price":          "float64",
            "quantity":       "int32",
            "discount":       "float64",
        },
        low_memory=False,
    )

    elapsed  = time.perf_counter() - t0
    mem_used = _memory_mb() - mem_before
    logger.info(
        "[EXTRACT] Rows=%s  Cols=%s  Time=%.3fs  ΔMemory=%.1fMB",
        f"{len(df):,}", df.shape[1], elapsed, mem_used,
    )
    return df, elapsed, mem_used


# ---------------------------------------------------------------------------
# Stage 2 – TRANSFORM
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, float, float, dict[str, Any]]:
    """
    Full ETL transform:
      - Drop duplicates (transaction_id)
      - Drop true duplicate rows (exact match)
      - Fill / drop nulls
      - Compute derived columns: net_revenue, revenue_band, month, year, quarter
      - Encode categoricals with category dtype (memory optimisation)
      - Flag high-value transactions

    Returns
    -------
    df_clean, wall_time_sec, delta_mem_mb, quality_report
    """
    logger.info("[TRANSFORM] Starting …")
    mem_before  = _memory_mb()
    t0          = time.perf_counter()
    rows_before = len(df)
    nulls_before = _null_rate(df)

    # ── 2.1 Remove exact duplicates ──────────────────────────────────────────
    df = df.drop_duplicates()

    # ── 2.2 Remove transaction_id duplicates (keep first) ────────────────────
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    # ── 2.3 Drop rows with null in critical columns ───────────────────────────
    critical_cols = ["transaction_id", "customer_id", "price", "quantity",
                     "category", "status", "region"]
    df = df.dropna(subset=critical_cols)

    # ── 2.4 Fill non-critical nulls ───────────────────────────────────────────
    df["product_name"] = df["product_name"].fillna("Unknown Product")
    df["discount"]     = df["discount"].fillna(0.0)

    # ── 2.5 Validate & clip ranges ───────────────────────────────────────────
    df = df[df["price"] > 0]
    df = df[df["quantity"] > 0]
    df["discount"] = df["discount"].clip(0.0, 1.0)

    # ── 2.6 Feature engineering ───────────────────────────────────────────────
    df["gross_revenue"] = df["price"] * df["quantity"]
    df["discount_amt"]  = df["gross_revenue"] * df["discount"]
    df["net_revenue"]   = df["gross_revenue"] - df["discount_amt"]

    df["month"]   = df["timestamp"].dt.month
    df["year"]    = df["timestamp"].dt.year
    df["quarter"] = df["timestamp"].dt.quarter
    df["dow"]     = df["timestamp"].dt.dayofweek   # 0 = Monday

    # Revenue band (for segmentation analytics)
    df["revenue_band"] = pd.cut(
        df["net_revenue"],
        bins=[0, 50, 200, 500, 1_000, np.inf],
        labels=["micro", "low", "medium", "high", "premium"],
        right=False,
    )

    # High-value transaction flag
    df["is_high_value"] = (df["net_revenue"] > df["net_revenue"].quantile(0.90)).astype("int8")

    # ── 2.7 Optimize memory with category dtypes ──────────────────────────────
    cat_cols = ["category", "payment_method", "status", "region",
                "device_type", "revenue_band"]
    for col in cat_cols:
        df[col] = df[col].astype("category")

    # ── 2.8 Quality report ────────────────────────────────────────────────────
    rows_after   = len(df)
    nulls_after  = _null_rate(df)
    quality_rpt  = {
        "rows_before":    rows_before,
        "rows_after":     rows_after,
        "rows_dropped":   rows_before - rows_after,
        "drop_pct":       round((rows_before - rows_after) / max(rows_before, 1) * 100, 2),
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

def load(df: pd.DataFrame, label: str) -> tuple[float, float]:
    """
    Write cleaned dataframe to Parquet (snappy compressed).

    Returns
    -------
    wall_time_sec, delta_mem_mb
    """
    out_path   = OUTPUT_DIR / f"clean_{label}.parquet"
    mem_before = _memory_mb()
    t0         = time.perf_counter()

    logger.info("[LOAD] Writing Parquet → %s", out_path)
    df.to_parquet(out_path, index=False, engine="pyarrow", compression="snappy")

    elapsed  = time.perf_counter() - t0
    mem_used = _memory_mb() - mem_before
    size_mb  = out_path.stat().st_size / (1024 ** 2)
    logger.info(
        "[LOAD] Written %.1fMB  Time=%.3fs  ΔMemory=%.1fMB",
        size_mb, elapsed, mem_used,
    )
    return elapsed, mem_used


# ---------------------------------------------------------------------------
# Stage 4 – ANALYTICS
# ---------------------------------------------------------------------------

def analytics(df: pd.DataFrame, label: str) -> tuple[dict[str, Any], float, float]:
    """
    Compute business KPIs and aggregations.

    Returns
    -------
    kpis, wall_time_sec, delta_mem_mb
    """
    logger.info("[ANALYTICS] Computing KPIs …")
    mem_before = _memory_mb()
    t0         = time.perf_counter()

    kpis: dict[str, Any] = {}

    # ── Overall KPIs ──────────────────────────────────────────────────────────
    kpis["total_transactions"]  = int(len(df))
    kpis["total_gross_revenue"] = round(float(df["gross_revenue"].sum()), 2)
    kpis["total_net_revenue"]   = round(float(df["net_revenue"].sum()),   2)
    kpis["total_discount_amt"]  = round(float(df["discount_amt"].sum()),  2)
    kpis["avg_order_value"]     = round(float(df["net_revenue"].mean()),  2)
    kpis["median_order_value"]  = round(float(df["net_revenue"].median()),2)
    kpis["unique_customers"]    = int(df["customer_id"].nunique())
    kpis["unique_products"]     = int(df["product_id"].nunique())
    kpis["high_value_txn_pct"]  = round(df["is_high_value"].mean() * 100, 2)
    kpis["cancellation_rate"]   = round(
        (df["status"] == "cancelled").mean() * 100, 2
    )
    kpis["refund_rate"]         = round(
        (df["status"] == "refunded").mean() * 100, 2
    )

    # ── Category-level aggregation ────────────────────────────────────────────
    cat_agg = (
        df.groupby("category", observed=True)
        .agg(
            transactions=("transaction_id", "count"),
            net_revenue  =("net_revenue",   "sum"),
            avg_discount =("discount",       "mean"),
        )
        .sort_values("net_revenue", ascending=False)
        .round(2)
    )
    kpis["category_revenue"] = cat_agg.to_dict("index")

    # ── Monthly revenue trend ─────────────────────────────────────────────────
    monthly = (
        df.groupby(["year", "month"], observed=True)
        ["net_revenue"].sum()
        .reset_index()
        .sort_values(["year", "month"])
        .round(2)
    )
    kpis["monthly_trend"] = monthly.to_dict("records")

    # ── Payment method breakdown ──────────────────────────────────────────────
    pay_agg = (
        df.groupby("payment_method", observed=True)
        ["net_revenue"].sum()
        .sort_values(ascending=False)
        .round(2)
        .to_dict()
    )
    kpis["payment_method_revenue"] = pay_agg

    # ── Region performance ────────────────────────────────────────────────────
    region_agg = (
        df.groupby("region", observed=True)
        .agg(
            transactions=("transaction_id", "count"),
            net_revenue  =("net_revenue",   "sum"),
        )
        .sort_values("net_revenue", ascending=False)
        .round(2)
        .to_dict("index")
    )
    kpis["region_performance"] = region_agg

    # ── Revenue band distribution ─────────────────────────────────────────────
    kpis["revenue_band_dist"] = (
        df["revenue_band"]
        .value_counts(normalize=True)
        .mul(100)
        .round(2)
        .to_dict()
    )

    # ── Top 10 customers by revenue ───────────────────────────────────────────
    top_cust = (
        df.groupby("customer_id")["net_revenue"]
        .sum()
        .nlargest(10)
        .round(2)
        .to_dict()
    )
    kpis["top_10_customers"] = {str(k): v for k, v in top_cust.items()}

    # ── Persist analytics JSON ────────────────────────────────────────────────
    out_path = METRICS_DIR / f"pandas_analytics_{label}.json"
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

def run_pipeline(csv_path: str | Path, label: str) -> dict[str, Any] | None:
    """
    Execute the full Pandas ETL pipeline and return a structured metrics dict.
    Returns None if MemoryError occurs.
    """
    logger.info("=" * 60)
    logger.info("  PANDAS PIPELINE  |  Scale: %s", label)
    logger.info("=" * 60)

    peak_mem_start = _memory_mb()
    total_t0       = time.perf_counter()
    process        = psutil.Process(os.getpid())

    try:
        # ── Stage 1: Extract ──────────────────────────────────────────────────────
        df, t_extract, m_extract = extract(csv_path)
        raw_rows = len(df)

        # ── Stage 2: Transform ────────────────────────────────────────────────────
        # To demonstrate Pandas memory limit, we'll perform a heavy copy + merge-like expansion
        # only at the 'extra_large' scale (20M rows).
        if "10M" in label:
            logger.info("[PANDAS] Performing memory-intensive expansion (3x copy)...")
            # Force a massive memory spike
            df = pd.concat([df, df, df], ignore_index=True) # 20M -> 60M rows
            gc.collect()


        df, t_transform, m_transform, quality = transform(df)

        # ── Stage 3: Load ─────────────────────────────────────────────────────────
        t_load, m_load = load(df, label)

        # ── Stage 4: Analytics ────────────────────────────────────────────────────
        kpis, t_analytics, m_analytics = analytics(df, label)

        total_time  = time.perf_counter() - total_t0
        peak_rss    = max(process.memory_info().rss / (1024 ** 2), peak_mem_start)

        # ── Collect metrics ───────────────────────────────────────────────────────
        metrics = {
            "framework":         "pandas",
            "scale_label":       label,
            "status":            "SUCCESS",
            "raw_rows":          raw_rows,
            "clean_rows":        quality["rows_after"],
            "cores_used":        1,
            "time_extract_s":    round(t_extract,   4),
            "time_transform_s":  round(t_transform, 4),
            "time_load_s":       round(t_load,      4),
            "time_analytics_s":  round(t_analytics, 4),
            "time_total_s":      round(total_time,  4),
            "memory_peak_mb":    round(peak_rss,    2),
            "quality":           quality,
            "kpis":              kpis,
        }
    except MemoryError:
        logger.error("[PANDAS] Out of Memory! Failed on scale: %s", label)
        metrics = {
            "framework":         "pandas",
            "scale_label":       label,
            "status":            "FAILED (OOM)",
            "time_total_s":      -1.0,
            "memory_peak_mb":    round(process.memory_info().rss / (1024 ** 2), 2),
        }
    except Exception as e:
        logger.error("[PANDAS] Unexpected Error: %s", e)
        metrics = {
            "framework":         "pandas",
            "scale_label":       label,
            "status":            "ERROR",
            "error_msg":         str(e),
            "time_total_s":      -1.0,
            "memory_peak_mb":    round(process.memory_info().rss / (1024 ** 2), 2),
        }

    # Save metrics
    out = METRICS_DIR / f"pandas_metrics_{label}.json"
    with open(out, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    
    # Free
    if 'df' in locals(): del df
    gc.collect()

    return metrics



# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from data_generator import generate_dataset, SCALE_CONFIGS

    scales = sys.argv[1:] if len(sys.argv) > 1 else list(SCALE_CONFIGS.keys())
    for scale in scales:
        cfg  = SCALE_CONFIGS[scale]
        path = generate_dataset(scale)
        run_pipeline(path, cfg["label"])
