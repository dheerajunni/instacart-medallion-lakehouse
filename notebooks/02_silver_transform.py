# Databricks notebook source
# Silver Layer — Cleaning, Typing & Quality Enforcement
# -------------------------------------------------------
# Reads from Bronze Delta, applies:
#   - Deduplication on primary keys
#   - Type casting and null handling
#   - Referential integrity checks
#   - Row-count reconciliation vs Bronze
#   - Derived column enrichment
# Writes clean, trusted data to Silver Delta.
#
# Prerequisite: Run 01_bronze_ingest.py first.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from utils.logger import get_logger
from utils.quality import (
    check_null_rate,
    check_duplicate_rate,
    check_referential_integrity,
    reconcile_row_counts,
    log_layer_stats,
    DataQualityError,
)

logger = get_logger("silver")

# COMMAND ----------

BRONZE_PATH = "/delta/instacart/bronze"
SILVER_PATH = "/delta/instacart/silver"

# Acceptable thresholds
NULL_THRESHOLD      = 0.001   # 0.1% — except days_since_prior_order
DUPLICATE_THRESHOLD = 0.0     # Zero tolerance on PKs
RECON_THRESHOLD     = 0.001   # 0.1% row count drift allowed

# COMMAND ----------

# ── ORDERS ────────────────────────────────────────────────────────────────

logger.info("Processing silver.orders ...")

bronze_orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")
bronze_count  = bronze_orders.count()

# 1. Deduplication — keep first occurrence per order_id
w_dedup = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
orders_deduped = (
    bronze_orders
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingested_at", "_source_file", "_bronze_version")
)

# 2. Type safety — already enforced at Bronze schema, confirm no nulls on required cols
check_null_rate(orders_deduped, "order_id",          threshold=0.0,   layer="silver")
check_null_rate(orders_deduped, "user_id",           threshold=0.0,   layer="silver")
check_null_rate(orders_deduped, "order_number",      threshold=0.0,   layer="silver")
check_null_rate(orders_deduped, "order_dow",         threshold=0.0,   layer="silver")
check_null_rate(orders_deduped, "order_hour_of_day", threshold=0.0,   layer="silver")
# days_since_prior_order: null is valid for first orders (~6%) — allow up to 7%
check_null_rate(orders_deduped, "days_since_prior_order", threshold=0.07, layer="silver")

# 3. Duplicate check on PK
check_duplicate_rate(orders_deduped, "order_id", threshold=DUPLICATE_THRESHOLD, layer="silver")

# 4. Enrichment — add derived columns
orders_silver = orders_deduped.withColumns({
    "is_first_order": F.col("order_number") == 1,
    "order_day_name": F.when(F.col("order_dow") == 0, "Saturday")
                       .when(F.col("order_dow") == 1, "Sunday")
                       .when(F.col("order_dow") == 2, "Monday")
                       .when(F.col("order_dow") == 3, "Tuesday")
                       .when(F.col("order_dow") == 4, "Wednesday")
                       .when(F.col("order_dow") == 5, "Thursday")
                       .otherwise("Friday"),
    "order_time_bucket": F.when(F.col("order_hour_of_day").between(5, 11),  "Morning")
                          .when(F.col("order_hour_of_day").between(12, 16), "Afternoon")
                          .when(F.col("order_hour_of_day").between(17, 20), "Evening")
                          .otherwise("Night"),
    "_silver_processed_at": F.current_timestamp(),
})

# 5. Row count reconciliation vs Bronze
silver_count = orders_silver.count()
reconcile_row_counts(bronze_count, silver_count, RECON_THRESHOLD, "orders")

# 6. Write
(
    orders_silver.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/orders")
)
log_layer_stats("silver", "orders", silver_count)
logger.info(f"  ✅  silver.orders: {silver_count:,} rows (bronze had {bronze_count:,})")

# COMMAND ----------

# ── ORDER_PRODUCTS ────────────────────────────────────────────────────────

logger.info("Processing silver.order_products ...")

bronze_op    = spark.read.format("delta").load(f"{BRONZE_PATH}/order_products")
bronze_op_count = bronze_op.count()

# Deduplication on composite key (order_id + product_id)
w_op = Window.partitionBy("order_id", "product_id").orderBy(F.monotonically_increasing_id())
op_deduped = (
    bronze_op
    .withColumn("_rn", F.row_number().over(w_op))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_ingested_at", "_source_file", "_bronze_version")
)

check_null_rate(op_deduped,       "order_id",          threshold=0.0,               layer="silver")
check_null_rate(op_deduped,       "product_id",        threshold=0.0,               layer="silver")
check_null_rate(op_deduped,       "add_to_cart_order", threshold=0.0,               layer="silver")
check_duplicate_rate(op_deduped,  "order_id",          threshold=0.99, layer="silver")  # many per order is normal

# Referential integrity: all order_ids must exist in orders
orders_ref = spark.read.format("delta").load(f"{SILVER_PATH}/orders").select("order_id")
check_referential_integrity(op_deduped, "order_id", orders_ref, "order_id", layer="silver")

# Enrich
op_silver = op_deduped.withColumns({
    "is_reordered":           F.col("reordered") == 1,
    "_silver_processed_at":   F.current_timestamp(),
})

silver_op_count = op_silver.count()
reconcile_row_counts(bronze_op_count, silver_op_count, RECON_THRESHOLD, "order_products")

(
    op_silver.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/order_products")
)
log_layer_stats("silver", "order_products", silver_op_count)
logger.info(f"  ✅  silver.order_products: {silver_op_count:,} rows")

# COMMAND ----------

# ── PRODUCTS ──────────────────────────────────────────────────────────────

logger.info("Processing silver.products ...")

bronze_products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")
p_count = bronze_products.count()

check_null_rate(bronze_products,      "product_id",   threshold=0.0, layer="silver")
check_null_rate(bronze_products,      "product_name", threshold=0.0, layer="silver")
check_duplicate_rate(bronze_products, "product_id",   threshold=0.0, layer="silver")

products_silver = bronze_products.drop("_ingested_at", "_source_file", "_bronze_version") \
    .withColumn("_silver_processed_at", F.current_timestamp())

(
    products_silver.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/products")
)
log_layer_stats("silver", "products", p_count)
logger.info(f"  ✅  silver.products: {p_count:,} rows")

# ── AISLES & DEPARTMENTS (small lookups) ──────────────────────────────────

for name in ["aisles", "departments"]:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{name}") \
        .drop("_ingested_at", "_source_file", "_bronze_version") \
        .withColumn("_silver_processed_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}/{name}")
    logger.info(f"  ✅  silver.{name}: {df.count():,} rows")

# COMMAND ----------

print("\n" + "="*55)
print("  SILVER TRANSFORMATION COMPLETE")
print("="*55)
print("All quality gates passed.")
print(f"\nSilver Delta tables written to: {SILVER_PATH}")
print("Next step: Run 03_gold_metrics.py")
