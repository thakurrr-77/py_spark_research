# Big Data Processing Benchmark — Pandas vs PySpark

**Authors:** Aayush Ranjan (2210991137) & Shubham Thakur (2210992370)  
**Institute:** Chitkara Institute of Engineering and Technology  
**Paper:** Implementation and Study of Scalable Big Data Processing and Analytics

---

## Project Structure

```
chitkara_research_pyspark/
├── data_generator.py       # Synthetic e-commerce dataset generator (10K / 100K / 1M rows)
├── pandas_pipeline.py      # Naive single-node ETL pipeline (Pandas)
├── pyspark_pipeline.py     # Scalable distributed ETL pipeline (PySpark)
├── benchmark.py            # Orchestrator — runs both, collects & compares metrics
├── visualize_results.py    # Standalone chart + HTML dashboard generator
├── run_quick.py            # Quick run (small scale only, ~1–2 min)
├── requirements.txt
├── data/                   # Generated CSV datasets (auto-created)
├── output/
│   ├── pandas/             # Cleaned Parquet files from Pandas pipeline
│   └── pyspark/            # Cleaned Parquet files from PySpark pipeline
├── metrics/                # Raw JSON metrics per run
└── reports/                # Charts (PNG) + Markdown + HTML report
```

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python      | ≥ 3.10  |
| Java JDK    | ≥ 11 (required by PySpark) |
| RAM         | ≥ 8 GB recommended |
| Disk        | ~3 GB for all three dataset scales |

### Install Java (Windows)

Download and install [Amazon Corretto 17](https://corretto.aws/downloads/latest/amazon-corretto-17-x64-windows-jdk.msi)  
or [Eclipse Temurin 17](https://adoptium.net/).

After installing, verify:
```powershell
java -version
```

### Install Python dependencies

```powershell
pip install -r requirements.txt
```

---

## Running the Benchmark

### Option A — Quick Demo (10K rows, ~1–2 min)
```powershell
python run_quick.py
```

### Option B — Specific scales
```powershell
# small = 10K, medium = 100K, large = 1M
python benchmark.py small medium
```

### Option C — Full benchmark (all scales, ~15–30 min)
```powershell
python benchmark.py
```

---

## Pipeline Stages (identical for both frameworks)

| Stage     | Description |
|-----------|-------------|
| Extract   | Read raw CSV with explicit schema |
| Transform | Dedup, null-drop/fill, clip ranges, feature engineering, revenue banding |
| Load      | Write Parquet (Snappy compressed) |
| Analytics | Category KPIs, monthly trend, payment/region breakdown, top customers |

---

## Metrics Captured

| Metric | Description |
|--------|-------------|
| `time_extract_s` | Wall-clock seconds for CSV read |
| `time_transform_s` | Wall-clock seconds for all transforms |
| `time_load_s` | Wall-clock seconds for Parquet write |
| `time_analytics_s` | Wall-clock seconds for KPI computation |
| `time_total_s` | End-to-end pipeline time |
| `memory_peak_mb` | Peak driver RSS (psutil) |
| `cores_used` | 1 for Pandas, N for PySpark (all cores) |
| `null_rate_before/after` | Data quality metric |
| `drop_pct` | % rows removed during cleaning |

---

## Outputs

| File | Description |
|------|-------------|
| `reports/benchmark_dashboard.png` | 6-panel chart overview |
| `reports/benchmark_report.html` | Dark-theme interactive HTML report |
| `reports/benchmark_report.md` | Markdown summary with tables |
| `reports/total_time_comparison.png` | Total time bar chart |
| `reports/speedup_factor.png` | PySpark speed-up line chart |
| `reports/memory_comparison.png` | Memory usage comparison |
| `reports/stage_breakdown_*.png` | Per-stage timing per scale |
| `metrics/combined_metrics.json` | All raw metrics |
| `metrics/pandas_analytics_*.json` | Pandas KPI results |
| `metrics/pyspark_analytics_*.json` | PySpark KPI results |

---

## Expected Results (Approximate)

> Results vary by hardware. Below is indicative, based on 4-core / 16 GB RAM machine.

| Scale | Pandas Total | PySpark Total | Speed-up |
|-------|-------------|---------------|----------|
| 10K   | ~0.5 s      | ~10 s         | 0.05× (Spark overhead dominates small data) |
| 100K  | ~2 s        | ~12 s         | ~0.2×   |
| 1M    | ~18 s       | ~20 s         | ~0.9–1.5× (Spark starts to catch up/win) |

> **Key insight:** PySpark has a ~10 s JVM startup overhead. At small scales Pandas wins.
> As data size grows to millions+ of rows, PySpark's parallel execution provides significant 
> benefits, especially on multi-core or cluster deployments.

---

## Troubleshooting

**`JAVA_HOME is not set`**  
→ Install Java and set the environment variable:
```powershell
[System.Environment]::SetEnvironmentVariable("JAVA_HOME","C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot","Machine")
```

**`winutils.exe` error on Windows**  
→ Download [winutils.exe](https://github.com/steveloughran/winutils) for your Hadoop version and:
```powershell
$env:HADOOP_HOME = "C:\hadoop"
```

**Memory errors on 1M dataset**  
→ Reduce `chunk_size` in `data_generator.py` or skip the `large` scale.
