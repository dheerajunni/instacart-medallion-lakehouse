"""
utils/quality.py
----------------
Data quality gate functions used across Bronze, Silver, and Gold layers.

Each function logs a result and raises DataQualityError if a threshold is
breached — halting the pipeline before bad data propagates downstream.

Designed to work with PySpark DataFrames in Databricks.
"""

from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from utils.logger import get_logger

logger = get_logger("quality")


class DataQualityError(Exception):
    """Raised when a data quality gate fails. Halts the pipeline."""
    pass


# ── Core Gate Functions ────────────────────────────────────────────────────

def check_null_rate(df, column: str, threshold: float, layer: str) -> float:
    """
    Fails if null rate in `column` exceeds `threshold`.

    Args:
        df:        PySpark DataFrame
        column:    Column name to check
        threshold: Maximum acceptable null rate (0.0–1.0)
        layer:     Layer name for logging ("bronze", "silver", "gold")

    Returns:
        Actual null rate (float)

    Raises:
        DataQualityError: If null rate > threshold
    """
    from pyspark.sql import functions as F

    total = df.count()
    nulls = df.filter(F.col(column).isNull()).count()
    null_rate = round(nulls / total, 6) if total > 0 else 0.0

    status = "PASS" if null_rate <= threshold else "FAIL"
    logger.info(
        "[%s] null_rate | %s | col=%s | rate=%.4f%% | threshold=%.4f%% | %s",
        layer, status, column, null_rate * 100, threshold * 100, status
    )

    if null_rate > threshold:
        raise DataQualityError(
            f"[{layer}] null_rate FAILED on column '{column}': "
            f"{null_rate:.4%} > threshold {threshold:.4%}. "
            f"({nulls:,} null rows out of {total:,})"
        )
    return null_rate


def check_duplicate_rate(df, column: str, threshold: float, layer: str) -> float:
    """
    Fails if duplicate rate in `column` exceeds `threshold`.
    Duplicate rate = rows with a duplicated value / total rows.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    total = df.count()
    w = Window.partitionBy(column)
    dupes = (
        df.withColumn("_cnt", F.count("*").over(w))
          .filter(F.col("_cnt") > 1)
          .count()
    )
    dup_rate = round(dupes / total, 6) if total > 0 else 0.0

    status = "PASS" if dup_rate <= threshold else "FAIL"
    logger.info(
        "[%s] duplicate_rate | %s | col=%s | rate=%.4f%% | threshold=%.4f%%",
        layer, status, column, dup_rate * 100, threshold * 100
    )

    if dup_rate > threshold:
        raise DataQualityError(
            f"[{layer}] duplicate_rate FAILED on column '{column}': "
            f"{dup_rate:.4%} > threshold {threshold:.4%}. "
            f"({dupes:,} duplicate rows out of {total:,})"
        )
    return dup_rate


def check_referential_integrity(
    child_df,
    child_col: str,
    parent_df,
    parent_col: str,
    layer: str,
    threshold: float = 0.0,
) -> float:
    """
    Fails if `child_col` in child_df contains values absent from `parent_col` in parent_df.
    """
    from pyspark.sql import functions as F

    total = child_df.filter(F.col(child_col).isNotNull()).count()
    orphans = (
        child_df
        .filter(F.col(child_col).isNotNull())
        .join(parent_df.select(parent_col), child_df[child_col] == parent_df[parent_col], "left_anti")
        .count()
    )
    orphan_rate = round(orphans / total, 6) if total > 0 else 0.0

    status = "PASS" if orphan_rate <= threshold else "FAIL"
    logger.info(
        "[%s] referential_integrity | %s | %s → %s | orphan_rate=%.4f%%",
        layer, status, child_col, parent_col, orphan_rate * 100
    )

    if orphan_rate > threshold:
        raise DataQualityError(
            f"[{layer}] referential_integrity FAILED: {child_col} → {parent_col}. "
            f"{orphans:,} orphaned rows ({orphan_rate:.4%} > threshold {threshold:.4%})"
        )
    return orphan_rate


def reconcile_row_counts(
    source_count: int,
    target_count: int,
    threshold: float,
    table_name: str,
) -> float:
    """
    Compares row counts between two layers.
    Fails if abs(diff) / source_count > threshold.

    Used as Bronze → Silver gate to catch unexpected row loss or explosion.
    """
    diff = abs(target_count - source_count)
    drift = round(diff / source_count, 6) if source_count > 0 else 0.0

    status = "PASS" if drift <= threshold else "FAIL"
    logger.info(
        "reconciliation | %s | %s | source=%d | target=%d | drift=%.4f%% | threshold=%.4f%%",
        table_name, status, source_count, target_count, drift * 100, threshold * 100
    )

    if drift > threshold:
        raise DataQualityError(
            f"reconciliation FAILED for '{table_name}': "
            f"source={source_count:,}, target={target_count:,}, diff={diff:,} "
            f"({drift:.4%} > threshold {threshold:.4%})"
        )
    return drift


def assert_row_count_nonzero(count: int, table_name: str) -> None:
    """Fails immediately if a table has zero rows — catches empty file ingestion."""
    if count == 0:
        raise DataQualityError(
            f"Table '{table_name}' has 0 rows after ingestion. "
            "Check that the source file exists and is not empty."
        )
    logger.info("assert_nonzero | PASS | %s | %d rows", table_name, count)


def log_layer_stats(layer: str, table: str, row_count: int) -> None:
    """Write layer completion stats — can be extended to push to a metrics store."""
    logger.info(
        "LAYER_COMPLETE | layer=%s | table=%s | rows=%d | timestamp=%s",
        layer, table, row_count, datetime.utcnow().isoformat()
    )
