"""
benchmark.py
-------------
Orchestrates the full comparative benchmark:
  1. Generates datasets (10K / 100K / 1M rows)
  2. Runs Pandas pipeline on each scale
  3. Runs PySpark pipeline on each scale (reuses one SparkSession)
  4. Produces a side-by-side metrics report (console + JSON + Markdown)
  5. Generates comparison charts (PNG)

Usage:
    python benchmark.py              # all scales
    python benchmark.py small medium # specific scales

Authors : Aayush Ranjan & Shubham Thakur
Institute: Chitkara Institute of Engineering and Technology
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")   # headless backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from tabulate import tabulate

from data_generator   import generate_dataset, SCALE_CONFIGS
from pandas_pipeline  import run_pipeline as run_pandas
from pyspark_pipeline import run_pipeline as run_pyspark, _build_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

METRICS_DIR = Path("metrics")
METRICS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

PALETTE = {
    "pandas":  "#4C72B0",
    "pyspark": "#DD8452",
}

def _bar_comparison(
    labels:     list[str],
    pandas_vals: list[float],
    spark_vals:  list[float],
    title:       str,
    ylabel:      str,
    filename:    str,
    log_scale:   bool = False,
) -> None:
    x      = np.arange(len(labels))
    width  = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))

    bars_p = ax.bar(x - width / 2, pandas_vals, width,
                    label="Pandas",  color=PALETTE["pandas"],  alpha=0.88, edgecolor="white")
    bars_s = ax.bar(x + width / 2, spark_vals,  width,
                    label="PySpark", color=PALETTE["pyspark"], alpha=0.88, edgecolor="white")

    def _annotate(bars: Any) -> None:
        for bar in bars:
            h = bar.get_height()
            ax.annotate(
                f"{h:,.2f}",
                xy=(bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 4), textcoords="offset points",
                ha="center", va="bottom", fontsize=8.5,
            )

    _annotate(bars_p)
    _annotate(bars_s)

    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)
    ax.legend(fontsize=10)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:,.1f}"))
    if log_scale:
        ax.set_yscale("log")
        ax.set_ylabel(ylabel + " (log scale)", fontsize=11)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    fig.tight_layout()
    out = REPORTS_DIR / filename
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Chart saved → %s", out)


def _speedup_line(
    labels:       list[str],
    speedup_vals: list[float],
    filename:     str,
) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(labels, speedup_vals, marker="o", linewidth=2.5,
            color=PALETTE["pyspark"], markerfacecolor="white",
            markeredgewidth=2, markersize=9)
    ax.axhline(1.0, color=PALETTE["pandas"], linestyle="--",
               linewidth=1.5, label="Pandas baseline (1×)")
    for x, y in zip(labels, speedup_vals):
        ax.annotate(
            f"{y:.2f}×",
            xy=(x, y), xytext=(0, 10), textcoords="offset points",
            ha="center", fontsize=9, color=PALETTE["pyspark"],
        )
    ax.set_title("PySpark Speed-up vs Pandas", fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Speed-up factor (×)", fontsize=11)
    ax.set_xlabel("Dataset scale", fontsize=11)
    ax.legend(fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(alpha=0.3, linestyle="--")
    fig.tight_layout()
    out = REPORTS_DIR / filename
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Chart saved → %s", out)


def _stage_breakdown(
    label:      str,
    pd_metrics: dict[str, float],
    sp_metrics: dict[str, float],
    filename:   str,
) -> None:
    stages = ["Extract", "Transform", "Load", "Analytics"]
    pd_vals = [
        pd_metrics["time_extract_s"],
        pd_metrics["time_transform_s"],
        pd_metrics["time_load_s"],
        pd_metrics["time_analytics_s"],
    ]
    sp_vals = [
        sp_metrics["time_extract_s"],
        sp_metrics["time_transform_s"],
        sp_metrics["time_load_s"],
        sp_metrics["time_analytics_s"],
    ]

    x     = np.arange(len(stages))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(x - width / 2, pd_vals, width, label="Pandas",  color=PALETTE["pandas"],  alpha=0.88, edgecolor="white")
    ax.bar(x + width / 2, sp_vals, width, label="PySpark", color=PALETTE["pyspark"], alpha=0.88, edgecolor="white")
    ax.set_title(f"Stage-wise Execution Time — {label}", fontsize=13, fontweight="bold")
    ax.set_ylabel("Time (seconds)", fontsize=11)
    ax.set_xticks(x)
    ax.set_xticklabels(stages, fontsize=11)
    ax.legend(fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    fig.tight_layout()
    out = REPORTS_DIR / filename
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info("Chart saved → %s", out)


# ---------------------------------------------------------------------------
# Report generators
# ---------------------------------------------------------------------------

def _build_summary_table(all_metrics: list[dict[str, Any]]) -> list[list[Any]]:
    rows = []
    for m in all_metrics:
        rows.append([
            m["framework"].capitalize(),
            m["scale_label"],
            f"{m['raw_rows']:,}",
            f"{m['clean_rows']:,}",
            m["cores_used"],
            f"{m['time_extract_s']:.3f}",
            f"{m['time_transform_s']:.3f}",
            f"{m['time_load_s']:.3f}",
            f"{m['time_analytics_s']:.3f}",
            f"{m['time_total_s']:.3f}",
            f"{m['memory_peak_mb']:.1f}",
            f"{m['quality']['null_rate_before']:.2f}%",
            f"{m['quality']['null_rate_after']:.2f}%",
            f"{m['quality']['drop_pct']:.2f}%",
        ])
    return rows


HEADERS = [
    "Framework", "Scale", "Raw Rows", "Clean Rows", "Cores",
    "Extract (s)", "Transform (s)", "Load (s)", "Analytics (s)", "Total (s)",
    "Peak Mem (MB)", "Null Before", "Null After", "Rows Dropped %",
]


def _print_table(rows: list[list[Any]]) -> None:
    print("\n" + "=" * 110)
    print("  BENCHMARK RESULTS -- Pandas vs PySpark ETL")
    print("=" * 110)
    print(tabulate(rows, headers=HEADERS, tablefmt="grid", stralign="right"))
    print()


def _save_markdown_report(
    rows:        list[list[Any]],
    all_metrics: list[dict[str, Any]],
) -> Path:
    md_lines = [
        "# Benchmark Report — Pandas vs PySpark ETL Pipeline",
        "",
        "**Authors:** Aayush Ranjan & Shubham Thakur  ",
        "**Institute:** Chitkara Institute of Engineering and Technology",
        "",
        "## Execution Summary",
        "",
        tabulate(rows, headers=HEADERS, tablefmt="github"),
        "",
        "## Speed-up Analysis",
        "",
    ]

    # Group by scale
    scales = sorted({m["scale_label"] for m in all_metrics})
    speedup_rows = []
    for scale in scales:
        pd_m = next((m for m in all_metrics if m["framework"] == "pandas" and m["scale_label"] == scale), None)
        sp_m = next((m for m in all_metrics if m["framework"] == "pyspark" and m["scale_label"] == scale), None)
        if pd_m and sp_m:
            su = pd_m["time_total_s"] / max(sp_m["time_total_s"], 1e-6)
            speedup_rows.append([scale, f"{pd_m['time_total_s']:.3f}s", f"{sp_m['time_total_s']:.3f}s", f"{su:.2f}×"])
    md_lines += [
        tabulate(speedup_rows,
                 headers=["Scale", "Pandas Total", "PySpark Total", "Speed-up"],
                 tablefmt="github"),
        "",
        "## Charts",
        "",
        "![Total Execution Time](total_time_comparison.png)",
        "![Speed-up Factor](speedup_factor.png)",
        "![Peak Memory Usage](memory_comparison.png)",
        "",
    ]
    for scale in scales:
        md_lines.append(f"![Stage Breakdown {scale}](stage_breakdown_{scale}.png)")
        md_lines.append("")

    out = REPORTS_DIR / "benchmark_report.md"
    out.write_text("\n".join(md_lines), encoding="utf-8")
    logger.info("Markdown report → %s", out)
    return out


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_benchmark(scales: list[str] | None = None) -> None:
    scales = scales or list(SCALE_CONFIGS.keys())
    logger.info("Starting benchmark for scales: %s", scales)

    all_metrics: list[dict[str, Any]] = []
    pandas_results:  dict[str, dict] = {}
    pyspark_results: dict[str, dict] = {}

    # ── Generate all datasets first ───────────────────────────────────────────
    csv_paths: dict[str, Path] = {}
    for sc in scales:
        csv_paths[sc] = generate_dataset(sc)

    # ── Pandas runs ───────────────────────────────────────────────────────────
    logger.info("\n%s\n  PANDAS BENCHMARK RUNS\n%s", "─" * 50, "─" * 50)
    for sc in scales:
        cfg = SCALE_CONFIGS[sc]
        m   = run_pandas(csv_paths[sc], cfg["label"])
        all_metrics.append(m)
        pandas_results[cfg["label"]] = m

    # ── PySpark runs (single Session for all scales) ──────────────────────────
    logger.info("\n%s\n  PYSPARK BENCHMARK RUNS\n%s", "─" * 50, "─" * 50)
    spark = _build_spark("Benchmark_Suite")
    try:
        for sc in scales:
            cfg = SCALE_CONFIGS[sc]
            m   = run_pyspark(csv_paths[sc], cfg["label"], spark=spark)
            all_metrics.append(m)
            pyspark_results[cfg["label"]] = m
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")

    # ── Save combined metrics ─────────────────────────────────────────────────
    combined_path = METRICS_DIR / "combined_metrics.json"
    with open(combined_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)
    logger.info("Combined metrics → %s", combined_path)

    # ── Build report table ────────────────────────────────────────────────────
    rows = _build_summary_table(all_metrics)
    _print_table(rows)

    # ── Charts ────────────────────────────────────────────────────────────────
    common_scales = [
        lbl for lbl in [SCALE_CONFIGS[s]["label"] for s in scales]
        if lbl in pandas_results and lbl in pyspark_results
    ]

    pd_total  = [pandas_results[l]["time_total_s"]    for l in common_scales]
    sp_total  = [pyspark_results[l]["time_total_s"]   for l in common_scales]
    pd_mem    = [pandas_results[l]["memory_peak_mb"]   for l in common_scales]
    sp_mem    = [pyspark_results[l]["memory_peak_mb"]  for l in common_scales]
    speedups  = [p / max(s, 1e-6) for p, s in zip(pd_total, sp_total)]

    _bar_comparison(
        common_scales, pd_total, sp_total,
        title    = "Total Execution Time — Pandas vs PySpark",
        ylabel   = "Time (seconds)",
        filename = "total_time_comparison.png",
    )
    _bar_comparison(
        common_scales, pd_mem, sp_mem,
        title    = "Peak Driver Memory (RSS) — Pandas vs PySpark",
        ylabel   = "Memory (MB)",
        filename = "memory_comparison.png",
    )
    _speedup_line(common_scales, speedups, "speedup_factor.png")

    for lbl in common_scales:
        _stage_breakdown(
            lbl,
            pandas_results[lbl],
            pyspark_results[lbl],
            filename=f"stage_breakdown_{lbl}.png",
        )

    _save_markdown_report(rows, all_metrics)

    # Speed-up summary to console
    print("\n  SPEED-UP SUMMARY (PySpark vs Pandas)\n" + "-" * 40)
    for lbl, su in zip(common_scales, speedups):
        symbol = ">>" if su > 1.0 else "<<"
        print(f"  {symbol}  {lbl:>8s}  ->  {su:6.2f}x speed-up")
    print()
    logger.info("Benchmark complete. Reports in: %s/", REPORTS_DIR)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    chosen = sys.argv[1:] if len(sys.argv) > 1 else None
    if chosen:
        invalid = [s for s in chosen if s not in SCALE_CONFIGS]
        if invalid:
            print(f"Invalid scales: {invalid}. Choose from: {list(SCALE_CONFIGS.keys())}")
            sys.exit(1)
    run_benchmark(chosen)
