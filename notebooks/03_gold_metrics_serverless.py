# Databricks notebook source
# Gold Layer — Serverless Compatible (SQL CTAS pattern)
# -------------------------------------------------------
# Uses CREATE OR REPLACE TABLE ... AS SELECT via spark.sql()
# which is fully supported on Databricks Serverless.
#
# Prerequisite: Run 02_silver_transform.py first.

# COMMAND ----------
# ── UTILS SETUP — run this cell first ─────────────────────────────────────

import os, sys

os.makedirs("/databricks/driver/utils", exist_ok=True)

with open("/databricks/driver/utils/logger.py", "w") as f:
    f.write('''
import logging, sys
def get_logger(name, level=logging.INFO):
    logger = logging.getLogger(f"medallion.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger
''')

with open("/databricks/driver/utils/__init__.py", "w") as f:
    f.write("from utils.logger import get_logger\n")

sys.path.insert(0, "/databricks/driver")
print("✅ Utils loaded")

# COMMAND ----------
# ── CONFIG ────────────────────────────────────────────────────────────────

from utils.logger import get_logger
from pyspark.sql import functions as F
from pyspark.sql import Window

logger = get_logger("gold")

SILVER_PATH = "/Volumes/main/default/instacart_data/delta/silver"
CATALOG     = "main"
SCHEMA      = "default"

# COMMAND ----------
# ── LOAD SILVER + REGISTER AS TEMP VIEWS ──────────────────────────────────
# Load each Silver Delta table and register as a temp SQL view.
# spark.sql() CTAS queries will read from these views.

logger.info("Loading Silver tables and registering temp views ...")

tables = ["orders", "order_products", "products", "aisles", "departments"]

for t in tables:
    (
        spark.read
        .format("delta")
        .load(f"{SILVER_PATH}/{t}")
        .createOrReplaceTempView(t)
    )
    logger.info(f"  ✅  Registered temp view: {t}")

# Quick row count check
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {t}").collect()[0]["cnt"]
    print(f"  {t:<25} {count:>12,} rows")

# COMMAND ----------
# ── GOLD: fct_orders ──────────────────────────────────────────────────────
# Fact table at order-product grain (~33M rows)
# Surrogate key: MD5 hash of order_id + product_id

logger.info("Building gold.fct_orders ...")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.instacart_fct_orders AS

SELECT
    md5(concat_ws('_', cast(op.order_id as string), cast(op.product_id as string))) AS order_product_key,

    -- Foreign keys
    op.order_id,
    op.product_id,
    o.user_id,
    p.aisle_id,
    p.department_id,

    -- Order context
    o.order_number,
    o.order_dow,
    o.order_day_name,
    o.order_hour_of_day,
    o.order_time_bucket,
    o.days_since_prior_order,
    o.is_first_order,
    o.eval_set,

    -- Product hierarchy
    p.product_name,
    a.aisle     AS aisle_name,
    d.department AS department_name,

    -- Measures
    op.add_to_cart_order,
    op.is_reordered,
    op.reordered,

    current_timestamp() AS _gold_computed_at

FROM order_products op
JOIN orders      o  ON op.order_id   = o.order_id
JOIN products    p  ON op.product_id = p.product_id
JOIN aisles      a  ON p.aisle_id    = a.aisle_id
JOIN departments d  ON p.department_id = d.department_id
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.instacart_fct_orders").collect()[0]["cnt"]
logger.info(f"  ✅  instacart_fct_orders: {count:,} rows")

# COMMAND ----------
# ── GOLD: dim_users ───────────────────────────────────────────────────────
# One row per user with lifetime behavior metrics and segments (~206K rows)

logger.info("Building gold.dim_users ...")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.instacart_dim_users AS

WITH order_stats AS (
    SELECT
        order_id,
        COUNT(*)                                        AS basket_size,
        SUM(reordered)                                  AS reordered_items,
        ROUND(SUM(reordered) / COUNT(*), 4)             AS order_reorder_rate
    FROM order_products
    GROUP BY order_id
),

user_agg AS (
    SELECT
        o.user_id,
        COUNT(DISTINCT o.order_id)                      AS total_orders,
        MAX(o.order_number)                             AS max_order_sequence,
        SUM(os.basket_size)                             AS lifetime_items,
        ROUND(AVG(os.basket_size), 2)                   AS avg_basket_size,
        ROUND(AVG(o.days_since_prior_order), 1)         AS avg_days_between_orders,
        ROUND(AVG(os.order_reorder_rate), 4)            AS avg_reorder_rate
    FROM orders o
    LEFT JOIN order_stats os ON o.order_id = os.order_id
    GROUP BY o.user_id
)

SELECT
    user_id,
    total_orders,
    max_order_sequence,
    lifetime_items,
    avg_basket_size,
    avg_days_between_orders,
    avg_reorder_rate,

    CASE
        WHEN total_orders >= 10 THEN 'High Frequency'
        WHEN total_orders >= 4  THEN 'Medium Frequency'
        ELSE 'Low Frequency'
    END AS order_frequency_segment,

    CASE
        WHEN avg_basket_size >= 15 THEN 'Large Basket'
        WHEN avg_basket_size >= 8  THEN 'Medium Basket'
        ELSE 'Small Basket'
    END AS basket_size_segment,

    CASE
        WHEN avg_reorder_rate >= 0.7 THEN 'Highly Habitual'
        WHEN avg_reorder_rate >= 0.4 THEN 'Mixed Explorer'
        ELSE 'Explorer'
    END AS reorder_behavior_segment,

    current_timestamp() AS _gold_computed_at

FROM user_agg
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.instacart_dim_users").collect()[0]["cnt"]
logger.info(f"  ✅  instacart_dim_users: {count:,} rows")

# COMMAND ----------
# ── GOLD: dim_products ────────────────────────────────────────────────────
# One row per product with reorder metrics and popularity tier (~50K rows)

logger.info("Building gold.dim_products ...")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.instacart_dim_products AS

WITH product_stats AS (
    SELECT
        op.product_id,
        COUNT(op.order_id)                              AS times_ordered,
        COUNT(DISTINCT o.user_id)                       AS unique_users,
        SUM(op.reordered)                               AS reorder_count,
        ROUND(AVG(CAST(op.reordered AS DOUBLE)), 4)     AS reorder_rate,
        ROUND(AVG(op.add_to_cart_order), 2)             AS avg_cart_position
    FROM order_products op
    JOIN orders o ON op.order_id = o.order_id
    GROUP BY op.product_id
)

SELECT
    p.product_id,
    p.product_name,
    a.aisle_id,
    a.aisle      AS aisle_name,
    d.department_id,
    d.department AS department_name,

    COALESCE(ps.times_ordered,   0)  AS times_ordered,
    COALESCE(ps.unique_users,    0)  AS unique_users,
    COALESCE(ps.reorder_count,   0)  AS reorder_count,
    COALESCE(ps.reorder_rate,    0)  AS reorder_rate,
    ps.avg_cart_position,

    CASE
        WHEN ps.times_ordered >= 10000 THEN 'Top Seller'
        WHEN ps.times_ordered >= 1000  THEN 'Mid Tier'
        ELSE 'Long Tail'
    END AS popularity_tier,

    current_timestamp() AS _gold_computed_at

FROM products p
LEFT JOIN aisles      a  ON p.aisle_id      = a.aisle_id
LEFT JOIN departments d  ON p.department_id = d.department_id
LEFT JOIN product_stats ps ON p.product_id  = ps.product_id
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.instacart_dim_products").collect()[0]["cnt"]
logger.info(f"  ✅  instacart_dim_products: {count:,} rows")

# COMMAND ----------
# ── GOLD: mart_dept_performance ───────────────────────────────────────────
# Department KPI aggregations — 21 rows (one per department)

logger.info("Building gold.mart_dept_performance ...")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.instacart_mart_dept_performance AS

WITH base AS (
    SELECT
        op.order_id,
        op.product_id,
        o.user_id,
        o.order_day_name,
        o.order_time_bucket,
        op.reordered,
        op.add_to_cart_order,
        p.department_id,
        d.department AS department_name
    FROM order_products op
    JOIN orders      o  ON op.order_id    = o.order_id
    JOIN products    p  ON op.product_id  = p.product_id
    JOIN departments d  ON p.department_id = d.department_id
    WHERE o.eval_set = 'prior'
),

totals AS (
    SELECT COUNT(*) AS grand_total FROM base
)

SELECT
    b.department_id,
    b.department_name,

    COUNT(DISTINCT b.order_id)      AS total_orders,
    COUNT(*)                        AS total_line_items,
    COUNT(DISTINCT b.user_id)       AS unique_users,
    COUNT(DISTINCT b.product_id)    AS unique_products,
    SUM(b.reordered)                AS reorder_line_items,

    ROUND(AVG(CAST(b.reordered AS DOUBLE)), 4)   AS reorder_rate,
    ROUND(AVG(b.add_to_cart_order), 2)           AS avg_cart_position,
    ROUND(COUNT(*) / t.grand_total, 6)           AS pct_of_total_items,

    -- Day-of-week split
    ROUND(AVG(CASE WHEN b.order_day_name = 'Saturday' THEN 1.0 ELSE 0.0 END), 4) AS pct_saturday,
    ROUND(AVG(CASE WHEN b.order_day_name = 'Sunday'   THEN 1.0 ELSE 0.0 END), 4) AS pct_sunday,

    -- Time-of-day split
    ROUND(AVG(CASE WHEN b.order_time_bucket = 'Morning'   THEN 1.0 ELSE 0.0 END), 4) AS pct_morning,
    ROUND(AVG(CASE WHEN b.order_time_bucket = 'Afternoon' THEN 1.0 ELSE 0.0 END), 4) AS pct_afternoon,
    ROUND(AVG(CASE WHEN b.order_time_bucket = 'Evening'   THEN 1.0 ELSE 0.0 END), 4) AS pct_evening,
    ROUND(AVG(CASE WHEN b.order_time_bucket = 'Night'     THEN 1.0 ELSE 0.0 END), 4) AS pct_night,

    current_timestamp() AS _gold_computed_at

FROM base b
CROSS JOIN totals t
GROUP BY b.department_id, b.department_name, t.grand_total
ORDER BY total_line_items DESC
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.instacart_mart_dept_performance").collect()[0]["cnt"]
logger.info(f"  ✅  instacart_mart_dept_performance: {count:,} rows")

# COMMAND ----------
# ── GOLD: mart_reorder_velocity ───────────────────────────────────────────
# Reorder rate by product at each order sequence position.
# Uses LAG window function — which products build habit fastest?

logger.info("Building gold.mart_reorder_velocity ...")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.instacart_mart_reorder_velocity AS

WITH sequence_base AS (
    SELECT
        op.product_id,
        p.product_name,
        d.department,
        o.user_id,
        o.order_number,
        op.reordered,
        LAG(op.reordered, 1, 0) OVER (
            PARTITION BY o.user_id, op.product_id
            ORDER BY o.order_number
        ) AS prev_reordered,
        ROW_NUMBER() OVER (
            PARTITION BY o.user_id, op.product_id
            ORDER BY o.order_number
        ) AS order_rank
    FROM order_products op
    JOIN orders      o  ON op.order_id    = o.order_id
    JOIN products    p  ON op.product_id  = p.product_id
    JOIN departments d  ON p.department_id = d.department_id
    WHERE o.eval_set != 'test'
),

bucketed AS (
    SELECT
        product_id,
        product_name,
        department,
        reordered,
        CASE
            WHEN order_rank = 1 THEN '1st_purchase'
            WHEN order_rank = 2 THEN '2nd_purchase'
            WHEN order_rank = 3 THEN '3rd_purchase'
            WHEN order_rank <= 5 THEN '4th_5th_purchase'
            ELSE '6th_plus_purchase'
        END AS order_position_bucket
    FROM sequence_base
)

SELECT
    product_id,
    product_name,
    department,
    order_position_bucket,
    COUNT(*)                                        AS purchase_count,
    ROUND(AVG(CAST(reordered AS DOUBLE)), 4)        AS reorder_rate_at_position,
    current_timestamp()                             AS _gold_computed_at
FROM bucketed
GROUP BY product_id, product_name, department, order_position_bucket
HAVING COUNT(*) >= 50
ORDER BY product_id, order_position_bucket
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.instacart_mart_reorder_velocity").collect()[0]["cnt"]
logger.info(f"  ✅  instacart_mart_reorder_velocity: {count:,} rows")

# COMMAND ----------
# ── FINAL SUMMARY ─────────────────────────────────────────────────────────

gold_tables = [
    "instacart_fct_orders",
    "instacart_dim_users",
    "instacart_dim_products",
    "instacart_mart_dept_performance",
    "instacart_mart_reorder_velocity",
]

print("\n" + "=" * 65)
print("  GOLD LAYER COMPLETE")
print("=" * 65)
for t in gold_tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {CATALOG}.{SCHEMA}.{t:<38}  {count:>12,} rows")
print("=" * 65)

# COMMAND ----------
# ── VERIFICATION QUERIES — run these individually ─────────────────────────

# Top departments by reorder rate
spark.sql(f"""
    SELECT department_name, reorder_rate, total_line_items, pct_of_total_items
    FROM {CATALOG}.{SCHEMA}.instacart_mart_dept_performance
    ORDER BY reorder_rate DESC
""").show(10, truncate=False)

# COMMAND ----------

# User segment breakdown
spark.sql(f"""
    SELECT order_frequency_segment, reorder_behavior_segment, COUNT(*) as user_count
    FROM {CATALOG}.{SCHEMA}.instacart_dim_users
    GROUP BY 1, 2
    ORDER BY user_count DESC
""").show(20)

# COMMAND ----------

# Top 10 most reordered products
spark.sql(f"""
    SELECT product_name, aisle_name, department_name, reorder_rate, times_ordered, avg_cart_position
    FROM {CATALOG}.{SCHEMA}.instacart_dim_products
    WHERE times_ordered >= 1000
    ORDER BY reorder_rate DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Products that build reorder habit fastest (highest rate at 2nd purchase)
spark.sql(f"""
    SELECT product_name, department, reorder_rate_at_position, purchase_count
    FROM {CATALOG}.{SCHEMA}.instacart_mart_reorder_velocity
    WHERE order_position_bucket = '2nd_purchase'
      AND purchase_count >= 500
    ORDER BY reorder_rate_at_position DESC
    LIMIT 10
""").show(truncate=False)
