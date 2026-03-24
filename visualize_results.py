"""
visualize_results.py
---------------------
Standalone visualizer: reads existing metrics JSONs and re-generates all charts
plus an HTML dashboard without re-running the pipelines.

Usage:
    python visualize_results.py

Authors : Aayush Ranjan & Shubham Thakur
Institute: Chitkara Institute of Engineering and Technology
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

METRICS_DIR = Path("metrics")
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

PALETTE = {"pandas": "#4C72B0", "pyspark": "#DD8452"}
HIGHLIGHT = "#2ecc71"


# ---------------------------------------------------------------------------
# Load metrics
# ---------------------------------------------------------------------------

def load_metrics() -> list[dict[str, Any]]:
    combined = METRICS_DIR / "combined_metrics.json"
    if combined.exists():
        with open(combined) as f:
            return json.load(f)

    # Fall back to individual files
    results = []
    for p in sorted(METRICS_DIR.glob("*_metrics_*.json")):
        with open(p) as f:
            results.append(json.load(f))
    return results


# ---------------------------------------------------------------------------
# Dashboard (single figure with 6 sub-plots)
# ---------------------------------------------------------------------------

def build_dashboard(metrics: list[dict[str, Any]]) -> None:
    pd_m  = [m for m in metrics if m["framework"] == "pandas"]
    sp_m  = [m for m in metrics if m["framework"] == "pyspark"]

    # Align on scale_label
    scales     = sorted({m["scale_label"] for m in pd_m + sp_m})
    pd_by_sc   = {m["scale_label"]: m for m in pd_m}
    sp_by_sc   = {m["scale_label"]: m for m in sp_m}
    common_sc  = [s for s in scales if s in pd_by_sc and s in sp_by_sc]

    if not common_sc:
        logger.warning("No common scales found between Pandas and PySpark results.")
        return

    pd_total   = [pd_by_sc[s]["time_total_s"]    for s in common_sc]
    sp_total   = [sp_by_sc[s]["time_total_s"]    for s in common_sc]
    pd_mem     = [pd_by_sc[s]["memory_peak_mb"]   for s in common_sc]
    sp_mem     = [sp_by_sc[s]["memory_peak_mb"]   for s in common_sc]
    speedups   = [p / max(s, 1e-6) for p, s in zip(pd_total, sp_total)]
    rows_raw   = [pd_by_sc[s]["raw_rows"]   for s in common_sc]
    rows_clean = [pd_by_sc[s]["clean_rows"] for s in common_sc]

    # ── PySpark stage breakdown for last (largest) scale ─────────────────────
    last_scale = common_sc[-1]
    stages     = ["Extract", "Transform", "Load", "Analytics"]
    pd_stages  = [
        pd_by_sc[last_scale]["time_extract_s"],
        pd_by_sc[last_scale]["time_transform_s"],
        pd_by_sc[last_scale]["time_load_s"],
        pd_by_sc[last_scale]["time_analytics_s"],
    ]
    sp_stages  = [
        sp_by_sc[last_scale]["time_extract_s"],
        sp_by_sc[last_scale]["time_transform_s"],
        sp_by_sc[last_scale]["time_load_s"],
        sp_by_sc[last_scale]["time_analytics_s"],
    ]

    fig = plt.figure(figsize=(18, 12))
    fig.suptitle(
        "Big Data Processing Benchmark — Pandas vs PySpark\n"
        "Chitkara Institute of Engineering and Technology",
        fontsize=15, fontweight="bold", y=0.98,
    )
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.42, wspace=0.38)

    x     = np.arange(len(common_sc))
    width = 0.35

    # ── Plot 1: Total time ────────────────────────────────────────────────────
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.bar(x - width / 2, pd_total, width, label="Pandas",  color=PALETTE["pandas"],  alpha=0.88)
    ax1.bar(x + width / 2, sp_total, width, label="PySpark", color=PALETTE["pyspark"], alpha=0.88)
    ax1.set_title("Total Execution Time (s)", fontweight="bold")
    ax1.set_xticks(x); ax1.set_xticklabels(common_sc)
    ax1.legend(fontsize=8); ax1.grid(axis="y", alpha=0.3)
    ax1.spines[["top", "right"]].set_visible(False)

    # ── Plot 2: Speed-up ──────────────────────────────────────────────────────
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(common_sc, speedups, marker="o", linewidth=2.5,
             color=PALETTE["pyspark"], markerfacecolor="white",
             markeredgewidth=2.5, markersize=9)
    ax2.axhline(1.0, color=PALETTE["pandas"], linestyle="--", linewidth=1.5, label="Baseline")
    for xi, su in zip(common_sc, speedups):
        ax2.annotate(f"{su:.2f}×", xy=(xi, su), xytext=(0, 8),
                     textcoords="offset points", ha="center", fontsize=9)
    ax2.set_title("PySpark Speed-up vs Pandas (×)", fontweight="bold")
    ax2.legend(fontsize=8); ax2.grid(alpha=0.3)
    ax2.spines[["top", "right"]].set_visible(False)

    # ── Plot 3: Memory usage ──────────────────────────────────────────────────
    ax3 = fig.add_subplot(gs[0, 2])
    ax3.bar(x - width / 2, pd_mem, width, label="Pandas",  color=PALETTE["pandas"],  alpha=0.88)
    ax3.bar(x + width / 2, sp_mem, width, label="PySpark", color=PALETTE["pyspark"], alpha=0.88)
    ax3.set_title("Peak RSS Memory (MB)", fontweight="bold")
    ax3.set_xticks(x); ax3.set_xticklabels(common_sc)
    ax3.legend(fontsize=8); ax3.grid(axis="y", alpha=0.3)
    ax3.spines[["top", "right"]].set_visible(False)

    # ── Plot 4: Stage breakdown ───────────────────────────────────────────────
    ax4 = fig.add_subplot(gs[1, 0])
    xs  = np.arange(len(stages))
    ax4.bar(xs - width / 2, pd_stages, width, label="Pandas",  color=PALETTE["pandas"],  alpha=0.88)
    ax4.bar(xs + width / 2, sp_stages, width, label="PySpark", color=PALETTE["pyspark"], alpha=0.88)
    ax4.set_title(f"Stage Breakdown @ {last_scale}", fontweight="bold")
    ax4.set_xticks(xs); ax4.set_xticklabels(stages, fontsize=9)
    ax4.legend(fontsize=8); ax4.grid(axis="y", alpha=0.3)
    ax4.spines[["top", "right"]].set_visible(False)

    # ── Plot 5: Row throughput ────────────────────────────────────────────────
    ax5 = fig.add_subplot(gs[1, 1])
    pd_throughput = [r / max(t, 1e-6) / 1000 for r, t in zip(rows_raw, pd_total)]
    sp_throughput = [r / max(t, 1e-6) / 1000 for r, t in zip(rows_raw, sp_total)]
    ax5.plot(common_sc, pd_throughput, marker="s", color=PALETTE["pandas"],
             linewidth=2, label="Pandas")
    ax5.plot(common_sc, sp_throughput, marker="o", color=PALETTE["pyspark"],
             linewidth=2, label="PySpark")
    ax5.set_title("Throughput (K rows/s)", fontweight="bold")
    ax5.legend(fontsize=8); ax5.grid(alpha=0.3)
    ax5.spines[["top", "right"]].set_visible(False)

    # ── Plot 6: Data quality ──────────────────────────────────────────────────
    ax6 = fig.add_subplot(gs[1, 2])
    drop_pcts = [pd_by_sc[s]["quality"]["drop_pct"] for s in common_sc]
    null_befores = [pd_by_sc[s]["quality"]["null_rate_before"] for s in common_sc]
    null_afters  = [pd_by_sc[s]["quality"]["null_rate_after"]  for s in common_sc]

    ax6.bar(x - width, drop_pcts,    width, label="Rows dropped %",  color="#e74c3c", alpha=0.8)
    ax6.bar(x,         null_befores, width, label="Null rate before", color="#f39c12", alpha=0.8)
    ax6.bar(x + width, null_afters,  width, label="Null rate after",  color=HIGHLIGHT, alpha=0.8)
    ax6.set_title("Data Quality Metrics (%)", fontweight="bold")
    ax6.set_xticks(x); ax6.set_xticklabels(common_sc)
    ax6.legend(fontsize=7.5); ax6.grid(axis="y", alpha=0.3)
    ax6.spines[["top", "right"]].set_visible(False)

    out = REPORTS_DIR / "benchmark_dashboard.png"
    fig.savefig(out, dpi=160, bbox_inches="tight")
    plt.close(fig)
    logger.info("Dashboard saved → %s", out)


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def build_html(metrics: list[dict[str, Any]]) -> None:
    pd_m = sorted(
        [m for m in metrics if m["framework"] == "pandas"],
        key=lambda m: m["time_total_s"],
    )
    sp_m = sorted(
        [m for m in metrics if m["framework"] == "pyspark"],
        key=lambda m: m["time_total_s"],
    )

    def _row(m: dict[str, Any]) -> str:
        q = m["quality"]
        return (
            f"<tr>"
            f"<td>{m['framework'].capitalize()}</td>"
            f"<td>{m['scale_label']}</td>"
            f"<td>{m['raw_rows']:,}</td>"
            f"<td>{m['clean_rows']:,}</td>"
            f"<td>{m['cores_used']}</td>"
            f"<td>{m['time_extract_s']:.3f}</td>"
            f"<td>{m['time_transform_s']:.3f}</td>"
            f"<td>{m['time_load_s']:.3f}</td>"
            f"<td>{m['time_analytics_s']:.3f}</td>"
            f"<td><strong>{m['time_total_s']:.3f}</strong></td>"
            f"<td>{m['memory_peak_mb']:.1f}</td>"
            f"<td>{q['null_rate_before']:.2f}%</td>"
            f"<td>{q['null_rate_after']:.2f}%</td>"
            f"<td>{q['drop_pct']:.2f}%</td>"
            f"</tr>"
        )

    rows_html = "\n".join(_row(m) for m in pd_m + sp_m)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Benchmark Report — Pandas vs PySpark</title>
  <style>
    :root {{
      --bg: #0f1117; --surface: #1a1d27; --accent: #4C72B0;
      --accent2: #DD8452; --text: #e8eaf6; --muted: #8892b0;
      --border: #2a2d3a; --radius: 12px; --green: #2ecc71;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
            line-height: 1.6; padding: 2rem; }}
    h1   {{ font-size: 2rem; margin-bottom:.3rem; background: linear-gradient(135deg, var(--accent), var(--accent2));
            -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
    .subtitle {{ color: var(--muted); margin-bottom: 2rem; }}
    .card {{ background: var(--surface); border-radius: var(--radius); padding: 1.5rem;
             border: 1px solid var(--border); margin-bottom: 1.5rem; }}
    .card h2 {{ margin-bottom: 1rem; font-size: 1.2rem; color: var(--accent); }}
    table {{ width:100%; border-collapse:collapse; font-size:.85rem; }}
    th {{ background: var(--border); padding:.6rem .8rem; text-align:left; color:var(--muted);
          font-weight:600; white-space:nowrap; }}
    tr:nth-child(even) {{ background: rgba(255,255,255,.03); }}
    td {{ padding:.5rem .8rem; border-bottom:1px solid var(--border); }}
    td:nth-child(1) {{ font-weight:700; }}
    img {{ max-width:100%; border-radius: var(--radius); margin-top:.5rem; }}
    .tag {{ display:inline-block; padding:.25rem .65rem; border-radius:99px;
            font-size:.75rem; font-weight:700; margin:.15rem; }}
    .tag.pandas  {{ background: #4C72B030; color: #4C72B0; border:1px solid #4C72B0; }}
    .tag.pyspark {{ background: #DD845230; color: #DD8452; border:1px solid #DD8452; }}
    footer {{ color:var(--muted); font-size:.8rem; margin-top:2rem; text-align:center; }}
  </style>
</head>
<body>
  <h1>Big Data Benchmark Report</h1>
  <p class="subtitle">
    Implementation and Study of Scalable Big Data Processing and Analytics<br/>
    <strong>Aayush Ranjan (2210991137) &amp; Shubham Thakur (2210992370)</strong> —
    Chitkara Institute of Engineering and Technology
  </p>

  <div class="card">
    <h2>📊 Benchmark Dashboard</h2>
    <img src="benchmark_dashboard.png" alt="Benchmark Dashboard"/>
  </div>

  <div class="card">
    <h2>📋 Detailed Results Table</h2>
    <table>
      <thead>
        <tr>
          <th>Framework</th><th>Scale</th><th>Raw Rows</th><th>Clean Rows</th><th>Cores</th>
          <th>Extract (s)</th><th>Transform (s)</th><th>Load (s)</th><th>Analytics (s)</th>
          <th>Total (s)</th><th>Mem (MB)</th><th>Null Before</th><th>Null After</th><th>Drop %</th>
        </tr>
      </thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>

  <div class="card">
    <h2>⚡ Speed-up Analysis</h2>
    <img src="speedup_factor.png" alt="Speed-up Factor"/>
    <img src="total_time_comparison.png" alt="Total Time Comparison"/>
  </div>

  <div class="card">
    <h2>💾 Memory Usage</h2>
    <img src="memory_comparison.png" alt="Memory Comparison"/>
  </div>

  <footer>
    Generated automatically by benchmark.py &mdash;
    <span class="tag pandas">Pandas</span>
    <span class="tag pyspark">PySpark</span>
  </footer>
</body>
</html>
"""
    out = REPORTS_DIR / "benchmark_report.html"
    out.write_text(html, encoding="utf-8")
    logger.info("HTML report → %s", out)


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    metrics = load_metrics()
    if not metrics:
        print("No metrics files found. Run benchmark.py first.")
    else:
        build_dashboard(metrics)
        build_html(metrics)
        print(f"\nAll charts and HTML report saved to: {REPORTS_DIR.resolve()}/")
