"""
setup_windows.py
-----------------
One-time setup script for Windows: downloads winutils.exe and hadoop.dll
required by PySpark on Windows, and sets HADOOP_HOME for the current session.

Run once before benchmark.py:
    python setup_windows.py

Authors : Aayush Ranjan & Shubham Thakur
"""

from __future__ import annotations

import os
import sys
import urllib.request
import shutil
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# We target the winutils that matches pyspark's bundled Hadoop version.
# PySpark 3.5.x ships with Hadoop 3.3.x libraries.
WINUTILS_BASE = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin"
)
FILES = ["winutils.exe", "hadoop.dll"]

HADOOP_HOME = Path("C:/hadoop")
BIN_DIR     = HADOOP_HOME / "bin"


def download_file(url: str, dest: Path) -> None:
    logger.info("Downloading %s → %s", url, dest)
    with urllib.request.urlopen(url) as resp, open(dest, "wb") as f:
        shutil.copyfileobj(resp, f)
    logger.info("OK  (%d bytes)", dest.stat().st_size)


def setup() -> None:
    if sys.platform != "win32":
        logger.info("Not Windows — winutils setup not needed.")
        return

    BIN_DIR.mkdir(parents=True, exist_ok=True)

    for fname in FILES:
        dest = BIN_DIR / fname
        if dest.exists():
            logger.info("Already exists: %s", dest)
            continue
        url = f"{WINUTILS_BASE}/{fname}"
        try:
            download_file(url, dest)
        except Exception as e:
            logger.error("Failed to download %s: %s", fname, e)
            logger.error(
                "Please manually download winutils.exe from:\n"
                "  https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin\n"
                "and place it in: %s", BIN_DIR
            )
            return

    # Set env vars for the current process (child processes inherit these)
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["PATH"]        = str(BIN_DIR) + os.pathsep + os.environ.get("PATH", "")

    logger.info("HADOOP_HOME set → %s", HADOOP_HOME)
    logger.info(
        "\nTo persist across sessions, run as Administrator:\n"
        '  [System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "%s", "Machine")\n'
        '  $path = [System.Environment]::GetEnvironmentVariable("Path","Machine")\n'
        '  [System.Environment]::SetEnvironmentVariable("Path", "%s;" + $path, "Machine")',
        HADOOP_HOME, BIN_DIR,
    )


if __name__ == "__main__":
    setup()
