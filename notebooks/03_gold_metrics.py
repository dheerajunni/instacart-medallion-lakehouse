# Databricks notebook source
# Gold Layer — Delivery Marketplace Metrics
# ------------------------------------------
# Reads from Silver Delta, computes analytics-ready aggregations:
#
#   gold.fct_orders          — order-product grain fact table
#   gold.dim_users           — user-level behavior and segmentation
#   gold.dim_products        — product popularity and reorder metrics
#   gold.mart_dept_perf      — department-level KPI aggregation
#   gold.mart_reorder_velocity — reorder velocity by product over order sequence
#
# All metrics computed using PySpark window functions.
# Prerequisite: Run 02_silver_transform.py first.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from utils.logger import get_logger
from utils.quality import check_null_rate, log_layer_stats, DataQualityError

logger = get_logger("gold")

# COMMAND ----------

SILVER_PATH = "/delta/instacart/silver"
GOLD_PATH   = "/delta/instacart/gold"

# COMMAND ----------

# ── LOAD SILVER TABLES ────────────────────────────────────────────────────

logger.info("Loading Silver tables ...")

orders       = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
order_prods  = spark.read.format("delta").load(f"{SILVER_PATH}/order_products")
products     = spark.read.format("delta").load(f"{SILVER_PATH}/products")
aisles       = spark.read.format("delta").load(f"{SILVER_PATH}/aisles")
departments  = spark.read.format("delta").load(f"{SILVER_PATH}/departments")

# Cache hot tables — used repeatedly across Gold computations
orders.cache()
order_prods.cache()
products.cache()
logger.info("Silver tables loaded and cached.")

# COMMAND ----------

# ── ENRICHED BASE JOIN ────────────────────────────────────────────────────
# Central enriched table joining all Silver sources.
# Used as the foundation for all Gold aggregations.

enriched = (
    order_prods
    .join(orders,      "order_id")
    .join(products,    "product_id")
    .join(aisles,      "aisle_id")
    .join(departments, "department_id")
    .select(
        # Keys
        "order_id", "product_id", "user_id", "aisle_id", "department_id",
        # Order context
        "order_number", "order_dow", "order_day_name", "order_hour_of_day",
        "order_time_bucket", "days_since_prior_order", "is_first_order", "eval_set",
        # Product hierarchy
        "product_name",
        F.col("aisle").alias("aisle_name"),
        F.col("department").alias("department_name"),
        # Measures
        "add_to_cart_order", "is_reordered", "reordered",
    )
)
enriched.cache()
logger.info(f"Enriched base: {enriched.count():,} rows")

# COMMAND ----------

# ── GOLD: fct_orders ──────────────────────────────────────────────────────
# Fact table at order-product grain.
# ~33M rows (prior + train eval sets combined)

logger.info("Computing gold.fct_orders ...")

# Surrogate key using hash of composite key
fct_orders = enriched.withColumn(
    "order_product_key",
    F.md5(F.concat_ws("_", F.col("order_id").cast("string"), F.col("product_id").cast("string")))
)

check_null_rate(fct_orders, "order_id",   threshold=0.0, layer="gold")
check_null_rate(fct_orders, "product_id", threshold=0.0, layer="gold")

(
    fct_orders.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("eval_set")              # Partition by eval_set for efficient downstream filtering
    .save(f"{GOLD_PATH}/fct_orders")
)
log_layer_stats("gold", "fct_orders", fct_orders.count())
logger.info(f"  ✅  gold.fct_orders written")

# COMMAND ----------

# ── GOLD: dim_users ───────────────────────────────────────────────────────
# One row per user with lifetime behavior metrics and segments.
# Window functions: avg, count, approx_percentile over user partition.

logger.info("Computing gold.dim_users ...")

# Per-order stats (basket size, reorder rate)
order_stats = (
    order_prods
    .groupBy("order_id")
    .agg(
        F.count("*").alias("basket_size"),
        F.sum(F.col("reordered").cast("int")).alias("reordered_items"),
    )
    .withColumn(
        "order_reorder_rate",
        F.col("reordered_items") / F.nullif(F.col("basket_size"), F.lit(0))
    )
)

orders_with_stats = orders.join(order_stats, "order_id", "left")

dim_users = (
    orders_with_stats
    .groupBy("user_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.max("order_number").alias("max_order_sequence"),
        F.sum("basket_size").alias("lifetime_items"),
        F.round(F.avg("basket_size"), 2).alias("avg_basket_size"),
        F.round(F.avg("days_since_prior_order"), 1).alias("avg_days_between_orders"),
        F.round(F.avg("order_reorder_rate"), 4).alias("avg_reorder_rate"),
    )
    .withColumns({
        "order_frequency_segment": F.when(F.col("total_orders") >= 10, "High Frequency")
                                    .when(F.col("total_orders") >= 4,  "Medium Frequency")
                                    .otherwise("Low Frequency"),
        "basket_size_segment":     F.when(F.col("avg_basket_size") >= 15, "Large Basket")
                                    .when(F.col("avg_basket_size") >= 8,  "Medium Basket")
                                    .otherwise("Small Basket"),
        "reorder_behavior_segment": F.when(F.col("avg_reorder_rate") >= 0.7, "Highly Habitual")
                                     .when(F.col("avg_reorder_rate") >= 0.4, "Mixed Explorer")
                                     .otherwise("Explorer"),
        "_gold_computed_at": F.current_timestamp(),
    })
)

(
    dim_users.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_users")
)
log_layer_stats("gold", "dim_users", dim_users.count())
logger.info(f"  ✅  gold.dim_users written")

# COMMAND ----------

# ── GOLD: dim_products ────────────────────────────────────────────────────
# One row per product with reorder rate, popularity tier, avg cart position.

logger.info("Computing gold.dim_products ...")

product_stats = (
    order_prods
    .groupBy("product_id")
    .agg(
        F.count("order_id").alias("times_ordered"),
        F.countDistinct(
            orders.join(order_prods, "order_id").select("user_id", "product_id")
                  .select("user_id")
        ).alias("placeholder"),   # replaced below
        F.sum(F.col("reordered").cast("int")).alias("reorder_count"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_rate"),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
    )
)

# Simpler unique user count via join
unique_users_per_product = (
    order_prods
    .join(orders.select("order_id", "user_id"), "order_id")
    .groupBy("product_id")
    .agg(F.countDistinct("user_id").alias("unique_users"))
)

product_stats_clean = (
    order_prods
    .groupBy("product_id")
    .agg(
        F.count("order_id").alias("times_ordered"),
        F.sum(F.col("reordered").cast("int")).alias("reorder_count"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_rate"),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
    )
    .join(unique_users_per_product, "product_id", "left")
)

dim_products = (
    products
    .join(aisles,       "aisle_id")
    .join(departments,  "department_id")
    .join(product_stats_clean, "product_id", "left")
    .withColumns({
        "popularity_tier": F.when(F.col("times_ordered") >= 10000, "Top Seller")
                            .when(F.col("times_ordered") >= 1000,  "Mid Tier")
                            .otherwise("Long Tail"),
        "_gold_computed_at": F.current_timestamp(),
    })
    .select(
        "product_id", "product_name",
        F.col("aisle").alias("aisle_name"), "aisle_id",
        F.col("department").alias("department_name"), "department_id",
        "times_ordered", "unique_users", "reorder_count",
        "reorder_rate", "avg_cart_position", "popularity_tier",
        "_gold_computed_at",
    )
)

(
    dim_products.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/dim_products")
)
log_layer_stats("gold", "dim_products", dim_products.count())
logger.info(f"  ✅  gold.dim_products written")

# COMMAND ----------

# ── GOLD: mart_dept_performance ───────────────────────────────────────────
# Department-level KPI aggregation for self-serve BI consumption.
# Answers: which departments drive volume, loyalty, and basket share?

logger.info("Computing gold.mart_dept_performance ...")

# Filter to prior eval_set (largest, most representative)
prior_enriched = enriched.filter(F.col("eval_set") == "prior")

# Window for department % of total (pct_of_grand_total)
total_items_overall = prior_enriched.count()

dept_perf = (
    prior_enriched
    .groupBy("department_id", "department_name")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_line_items"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("product_id").alias("unique_products"),
        F.sum(F.col("reordered").cast("int")).alias("reorder_line_items"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_rate"),
        F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
        # Time distribution
        F.round(F.avg(F.when(F.col("order_day_name") == "Saturday", 1).otherwise(0)), 4).alias("pct_saturday"),
        F.round(F.avg(F.when(F.col("order_day_name") == "Sunday",   1).otherwise(0)), 4).alias("pct_sunday"),
        F.round(F.avg(F.when(F.col("order_time_bucket") == "Morning",   1).otherwise(0)), 4).alias("pct_morning"),
        F.round(F.avg(F.when(F.col("order_time_bucket") == "Afternoon", 1).otherwise(0)), 4).alias("pct_afternoon"),
        F.round(F.avg(F.when(F.col("order_time_bucket") == "Evening",   1).otherwise(0)), 4).alias("pct_evening"),
        F.round(F.avg(F.when(F.col("order_time_bucket") == "Night",     1).otherwise(0)), 4).alias("pct_night"),
        F.current_timestamp().alias("_gold_computed_at"),
    )
    .withColumn(
        "pct_of_total_items",
        F.round(F.col("total_line_items") / F.lit(total_items_overall), 6)
    )
    .orderBy(F.col("total_line_items").desc())
)

(
    dept_perf.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/mart_dept_performance")
)
log_layer_stats("gold", "mart_dept_performance", dept_perf.count())
logger.info(f"  ✅  gold.mart_dept_performance written")

# COMMAND ----------

# ── GOLD: mart_reorder_velocity ───────────────────────────────────────────
# Reorder velocity by product across order sequence positions.
# Uses LAG window function to track cumulative reorder behavior.
# Answers: which products build reorder habit fastest?

logger.info("Computing gold.mart_reorder_velocity ...")

# Join order sequence onto order-product level
sequence_base = (
    order_prods
    .join(orders.select("order_id", "user_id", "order_number"), "order_id")
    .join(products.select("product_id", "product_name", "department_id"), "product_id")
    .join(departments, "department_id")
    .filter(F.col("eval_set").isNull() | (F.col("eval_set") != "test"))
)

# Window: per user+product, order by sequence to compute lag
w_seq = Window.partitionBy("user_id", "product_id").orderBy("order_number")

velocity_base = (
    sequence_base
    .withColumns({
        "prev_reordered": F.lag("reordered", 1, 0).over(w_seq),
        "order_rank":     F.row_number().over(w_seq),
    })
)

# Aggregate: for each product, avg reorder rate at order position 1, 2, 3, 4, 5+
reorder_velocity = (
    velocity_base
    .withColumn("order_position_bucket",
        F.when(F.col("order_rank") == 1, "1st_purchase")
         .when(F.col("order_rank") == 2, "2nd_purchase")
         .when(F.col("order_rank") == 3, "3rd_purchase")
         .when(F.col("order_rank") <= 5, "4th_5th_purchase")
         .otherwise("6th_plus_purchase")
    )
    .groupBy("product_id", "product_name", "department", "order_position_bucket")
    .agg(
        F.count("*").alias("purchase_count"),
        F.round(F.avg(F.col("reordered").cast("double")), 4).alias("reorder_rate_at_position"),
    )
    .filter(F.col("purchase_count") >= 50)       # Statistical minimum
    .withColumn("_gold_computed_at", F.current_timestamp())
)

(
    reorder_velocity.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/mart_reorder_velocity")
)
log_layer_stats("gold", "mart_reorder_velocity", reorder_velocity.count())
logger.info(f"  ✅  gold.mart_reorder_velocity written")

# COMMAND ----------

# ── FINAL SUMMARY ─────────────────────────────────────────────────────────

gold_tables = ["fct_orders", "dim_users", "dim_products", "mart_dept_performance", "mart_reorder_velocity"]

print("\n" + "="*55)
print("  GOLD LAYER COMPLETE")
print("="*55)
for t in gold_tables:
    count = spark.read.format("delta").load(f"{GOLD_PATH}/{t}").count()
    print(f"  gold.{t:<30} {count:>12,} rows")
print("="*55)
print(f"\nGold Delta tables written to: {GOLD_PATH}")
print("\nReady for BI consumption — connect Tableau/Power BI to Gold layer.")
