# Databricks notebook source
# Bronze Layer — Raw CSV Ingestion into Delta
# --------------------------------------------
# Ingests raw Instacart CSVs as-is into Delta tables with:
#   - Schema enforcement
#   - Ingestion timestamps
#   - Row count logging for downstream reconciliation
#
# In Databricks Community Edition:
#   1. Upload CSVs to DBFS via Data > Add Data > Upload
#   2. Update DATA_PATH below to match your upload path
#   3. Run all cells top to bottom

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType
)
from utils.logger import get_logger
from utils.quality import assert_row_count_nonzero, log_layer_stats

logger = get_logger("bronze")

# COMMAND ----------

# CONFIG — update DATA_PATH to your DBFS upload location
DATA_PATH   = "/FileStore/instacart"       # e.g. dbfs:/FileStore/instacart
BRONZE_PATH = "/delta/instacart/bronze"    # Delta table output path

# COMMAND ----------

# ── SCHEMAS ────────────────────────────────────────────────────────────────

orders_schema = StructType([
    StructField("order_id",                IntegerType(), nullable=False),
    StructField("user_id",                 IntegerType(), nullable=False),
    StructField("eval_set",                StringType(),  nullable=False),
    StructField("order_number",            IntegerType(), nullable=False),
    StructField("order_dow",               IntegerType(), nullable=False),
    StructField("order_hour_of_day",       IntegerType(), nullable=False),
    StructField("days_since_prior_order",  FloatType(),   nullable=True),   # null for first orders
])

order_products_schema = StructType([
    StructField("order_id",          IntegerType(), nullable=False),
    StructField("product_id",        IntegerType(), nullable=False),
    StructField("add_to_cart_order", IntegerType(), nullable=False),
    StructField("reordered",         IntegerType(), nullable=False),
])

products_schema = StructType([
    StructField("product_id",    IntegerType(), nullable=False),
    StructField("product_name",  StringType(),  nullable=False),
    StructField("aisle_id",      IntegerType(), nullable=False),
    StructField("department_id", IntegerType(), nullable=False),
])

aisles_schema = StructType([
    StructField("aisle_id", IntegerType(), nullable=False),
    StructField("aisle",    StringType(),  nullable=False),
])

departments_schema = StructType([
    StructField("department_id", IntegerType(), nullable=False),
    StructField("department",    StringType(),  nullable=False),
])

# COMMAND ----------

# ── INGESTION FUNCTION ─────────────────────────────────────────────────────

def ingest_to_bronze(filename: str, schema: StructType, table_name: str) -> int:
    """
    Read a raw CSV, attach ingestion metadata, and write to Bronze Delta.
    Returns row count for reconciliation logging.
    """
    logger.info(f"Ingesting {filename} → bronze.{table_name}")

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(f"{DATA_PATH}/{filename}")
    )

    # Attach bronze metadata columns
    df_bronze = df.withColumns({
        "_ingested_at":    F.current_timestamp(),
        "_source_file":    F.lit(filename),
        "_bronze_version": F.lit("1.0"),
    })

    # Write to Delta — overwrite for idempotent reruns
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{BRONZE_PATH}/{table_name}")
    )

    row_count = df_bronze.count()
    assert_row_count_nonzero(row_count, table_name)
    log_layer_stats("bronze", table_name, row_count)
    logger.info(f"  ✅  bronze.{table_name}: {row_count:,} rows written")
    return row_count

# COMMAND ----------

# ── RUN INGESTION ──────────────────────────────────────────────────────────

bronze_counts = {}

bronze_counts["orders"] = ingest_to_bronze(
    "orders.csv", orders_schema, "orders"
)

# Ingest prior and train separately for reconciliation, then combine
prior_count = ingest_to_bronze(
    "order_products__prior.csv", order_products_schema, "order_products_prior"
)
train_count = ingest_to_bronze(
    "order_products__train.csv", order_products_schema, "order_products_train"
)
bronze_counts["order_products_combined"] = prior_count + train_count

# Write combined order_products table
prior_df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_products_prior")
train_df = spark.read.format("delta").load(f"{BRONZE_PATH}/order_products_train")
combined_df = prior_df.union(train_df)
(
    combined_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{BRONZE_PATH}/order_products")
)
logger.info(f"  ✅  bronze.order_products (combined): {bronze_counts['order_products_combined']:,} rows")

bronze_counts["products"]    = ingest_to_bronze("products.csv",    products_schema,    "products")
bronze_counts["aisles"]      = ingest_to_bronze("aisles.csv",      aisles_schema,      "aisles")
bronze_counts["departments"] = ingest_to_bronze("departments.csv", departments_schema, "departments")

# COMMAND ----------

# ── SUMMARY ───────────────────────────────────────────────────────────────

print("\n" + "="*55)
print("  BRONZE INGESTION COMPLETE")
print("="*55)
for table, count in bronze_counts.items():
    print(f"  {table:<30} {count:>12,} rows")
print("="*55)
print(f"\nBronze Delta tables written to: {BRONZE_PATH}")
print("Next step: Run 02_silver_transform.py")
