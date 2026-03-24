"""
data_generator.py
-----------------
Generates synthetic e-commerce transaction datasets at multiple scale levels
for benchmarking Pandas vs PySpark ETL pipelines.

Authors : Aayush Ranjan & Shubham Thakur
Institute: Chitkara Institute of Engineering and Technology
"""

import os
import time
import random
import logging
import numpy as np
import pandas as pd
from faker import Faker
from tqdm import tqdm
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

fake = Faker()
random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Kitchen",
    "Sports", "Toys", "Grocery", "Automotive", "Health", "Beauty",
]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "upi", "net_banking", "cod"]
STATUSES = ["completed", "pending", "cancelled", "refunded"]
REGIONS = ["North", "South", "East", "West", "Central"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]

SCALE_CONFIGS = {
    "small":       {"rows": 10_000,     "label": "10K"},
    "medium":      {"rows": 100_000,    "label": "100K"},
    "large":       {"rows": 1_000_000,  "label": "1M"},
    "extra_large": {"rows": 10_000_000, "label": "10M"},
}


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def _generate_chunk(n: int, start_id: int = 0) -> pd.DataFrame:
    """Return a DataFrame chunk of *n* synthetic transaction rows."""
    ids = np.arange(start_id, start_id + n)
    categories = np.random.choice(CATEGORIES, size=n)

    # Introduce realistic noise / missing values
    prices = np.round(np.random.exponential(scale=150, size=n) + 5, 2)
    quantities = np.random.randint(1, 11, size=n)
    discounts = np.where(
        np.random.random(n) < 0.3,
        np.round(np.random.uniform(0.05, 0.40, n), 2),
        0.0,
    )

    # Inject ~2 % nulls into customer_email and product_name (dirty data)
    customer_ids = np.random.randint(1000, 99999, size=n)
    product_ids  = np.random.randint(100, 9999,  size=n)

    product_names = np.array([fake.bs().title() for _ in range(min(n, 500))])
    product_names = np.random.choice(product_names, size=n)
    null_mask_product = np.random.random(n) < 0.02
    product_names[null_mask_product] = None            # type: ignore[call-overload]

    emails = np.array([fake.email() for _ in range(min(n, 500))])
    emails = np.random.choice(emails, size=n)
    null_mask_email = np.random.random(n) < 0.02
    emails[null_mask_email] = None                      # type: ignore[call-overload]

    # Duplicate ~1 % of rows (simulate upstream duplication bugs)
    dup_flags = np.random.random(n) < 0.01

    ts_low     = int(pd.Timestamp("2022-01-01").value // 1_000_000_000) * 1_000_000_000
    ts_high    = int(pd.Timestamp("2024-12-31").value // 1_000_000_000) * 1_000_000_000
    ts_seconds = np.random.default_rng(seed=None).integers(
        ts_low // 1_000_000_000,
        ts_high // 1_000_000_000,
        size=n,
    )
    # Convert seconds to nanoseconds (pandas datetime unit)
    timestamps = pd.to_datetime(ts_seconds * 1_000_000_000)

    df = pd.DataFrame({
        "transaction_id":   ids,
        "customer_id":      customer_ids,
        "product_id":       product_ids,
        "product_name":     product_names,
        "category":         categories,
        "price":            prices,
        "quantity":         quantities,
        "discount":         discounts,
        "payment_method":   np.random.choice(PAYMENT_METHODS, size=n),
        "status":           np.random.choice(STATUSES, size=n),
        "region":           np.random.choice(REGIONS,  size=n),
        "device_type":      np.random.choice(DEVICE_TYPES, size=n),
        "timestamp":        timestamps,
        "is_duplicate_flag": dup_flags,       # meta column – not used in ETL
    })
    return df


def generate_dataset(scale: str = "small", chunk_size: int = 50_000) -> Path:
    """
    Generate and persist a CSV dataset for the given scale.

    Parameters
    ----------
    scale : str
        One of 'small', 'medium', 'large'.
    chunk_size : int
        Number of rows to generate per chunk (controls peak RAM usage).

    Returns
    -------
    Path
        Absolute path to the generated CSV file.
    """
    cfg     = SCALE_CONFIGS[scale]
    total   = cfg["rows"]
    label   = cfg["label"]
    out_csv = DATA_DIR / f"transactions_{label}.csv"

    if out_csv.exists():
        logger.info("Dataset already exists: %s — skipping generation.", out_csv)
        return out_csv

    logger.info("Generating %s-row dataset (%s) …", f"{total:,}", label)
    t0 = time.perf_counter()

    chunks  = []
    written = 0
    first   = True

    with tqdm(total=total, unit="rows", desc=f"Gen {label}") as pbar:
        while written < total:
            n     = min(chunk_size, total - written)
            chunk = _generate_chunk(n, start_id=written)
            chunks.append(chunk)
            written += n
            pbar.update(n)

            # Flush to disk every 5 chunks to avoid OOM on large scale
            if len(chunks) >= 5 or written >= total:
                mode   = "w" if first else "a"
                header = first
                df_out = pd.concat(chunks).copy()
                # Write timestamps as strings with second precision (Spark-compatible)
                df_out["timestamp"] = df_out["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
                df_out.to_csv(out_csv, mode=mode, header=header, index=False)
                chunks = []
                first  = False

    elapsed = time.perf_counter() - t0
    size_mb = out_csv.stat().st_size / (1024 ** 2)
    logger.info(
        "Dataset written → %s  (%.1f MB, %.2f s)",
        out_csv, size_mb, elapsed,
    )
    return out_csv


def generate_all() -> dict[str, Path]:
    """Generate datasets for all scale levels and return their paths."""
    paths = {}
    for scale in SCALE_CONFIGS:
        paths[scale] = generate_dataset(scale)
    return paths


if __name__ == "__main__":
    generate_all()
