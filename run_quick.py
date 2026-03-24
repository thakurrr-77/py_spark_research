"""
run_quick.py
------------
Quick-start script: runs only the SMALL scale (10K rows) for a fast end-to-end
sanity check and demo.  Takes ~1–2 minutes with Java/PySpark installed.

Usage:
    python run_quick.py

Authors : Aayush Ranjan & Shubham Thakur
"""

from benchmark import run_benchmark
from visualize_results import load_metrics, build_dashboard, build_html

if __name__ == "__main__":
    run_benchmark(scales=["small"])
    metrics = load_metrics()
    build_dashboard(metrics)
    build_html(metrics)
    print("\nDone! Open reports/benchmark_report.html in your browser.")
