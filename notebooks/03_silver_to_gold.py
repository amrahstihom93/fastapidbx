# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Silver to Gold - Dimensional Modeling (Star Schema)
# MAGIC This notebook aggregates and models Silver Delta tables into an enterprise-ready **Star Schema** stored as Gold Delta tables:
# MAGIC - **`dim_users`**: Cleaned, unique user records.
# MAGIC - **`dim_products`**: Cleaned, unique product records.
# MAGIC - **`dim_dates`**: Automatically generated calendar dimension.
# MAGIC - **`fact_orders`**: Transactional fact table flattened at the **order line-item level** for granular analysis.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# COMMAND ----------

# Setup catalog widgets
dbutils.widgets.text("catalog_name", "hive_metastore", "Catalog Name (hive_metastore / freeapi_catalog)")
CATALOG = dbutils.widgets.get("catalog_name")

# Define namespaces
if CATALOG.lower() == "hive_metastore":
    SILVER_SCHEMA = f"{CATALOG}.freeapi_silver"
    GOLD_SCHEMA   = f"{CATALOG}.freeapi_gold"
else:
    SILVER_SCHEMA = f"{CATALOG}.silver"
    GOLD_SCHEMA   = f"{CATALOG}.gold"

print(f"Reading from Silver Schema: {SILVER_SCHEMA}")
print(f"Writing to Gold Schema: {GOLD_SCHEMA}")

# Ensure Gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Dimension Tables: Users & Products

# COMMAND ----------

print("Generating dim_users and dim_products...")

# dim_users
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_SCHEMA}.dim_users AS
SELECT 
  email,
  username,
  role,
  created_at,
  updated_at
FROM {SILVER_SCHEMA}.users
""")

# dim_products
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_SCHEMA}.dim_products AS
SELECT 
  product_id,
  name,
  description,
  price,
  stock,
  category_id,
  main_image_url,
  created_at,
  updated_at
FROM {SILVER_SCHEMA}.products
""")

print("Dimensions dim_users and dim_products generated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Dimension Table: Dates (Calendar Dimension)

# COMMAND ----------

print("Generating dim_dates dimension...")

# Generate a sequence of dates from 2025-01-01 to 2027-12-31
start_date = "2025-01-01"
end_date = "2027-12-31"

date_df = (spark.range(0, 365 * 3)
    .withColumn("full_date", F.expr(f"date_add('{start_date}', cast(id as int))"))
    .filter(F.col("full_date") <= end_date)
    .select(
        F.date_format(F.col("full_date"), "yyyyMMdd").cast("integer").alias("date_key"),
        F.col("full_date"),
        F.year(F.col("full_date")).alias("year"),
        F.month(F.col("full_date")).alias("month"),
        F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
        F.dayofmonth(F.col("full_date")).alias("day_of_month"),
        F.dayofweek(F.col("full_date")).alias("day_of_week"),
        F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
        F.quarter(F.col("full_date")).alias("quarter"),
        F.expr("case when dayofweek(full_date) in (1, 7) then true else false end").alias("is_weekend")
    )
)

date_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_SCHEMA}.dim_dates")
print("Dimension dim_dates generated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Fact Table: Orders (Granular Line-Item Grain)

# COMMAND ----------

print("Generating fact_orders table...")

silver_orders = spark.table(f"{SILVER_SCHEMA}.orders")
silver_products = spark.table(f"{SILVER_SCHEMA}.products")
silver_users = spark.table(f"{SILVER_SCHEMA}.users")

# Explode orders items to get one row per product in an order
exploded_orders = (silver_orders
    .withColumn("item", F.explode(F.col("order_items")))
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("address_id"),
        F.col("order_status"),
        F.col("payment_provider"),
        F.col("payment_id"),
        F.col("is_payment_done"),
        F.col("original_price").alias("order_total_original"),
        F.col("discounted_price").alias("order_total_discounted"),
        F.col("coupon_id"),
        F.col("created_at").alias("order_time"),
        F.date_format(F.col("created_at"), "yyyyMMdd").cast("integer").alias("date_key"),
        F.col("item.productId").alias("product_id"),
        F.col("item.quantity").cast("integer").alias("quantity")
    )
)

# Join with users to resolve email (business key) and products to get unit price and calculate subtotals
fact_orders_df = (exploded_orders
    .join(silver_users, exploded_orders.customer_id == silver_users.username, "left") # customer_id is username in FreeAPI
    .join(silver_products, exploded_orders.product_id == silver_products.product_id, "left")
    .select(
        exploded_orders.order_id,
        silver_users.email.alias("user_email"),
        exploded_orders.address_id,
        exploded_orders.product_id,
        exploded_orders.quantity,
        silver_products.price.alias("unit_price"),
        (exploded_orders.quantity * silver_products.price).cast("decimal(10,2)").alias("item_subtotal"),
        exploded_orders.order_total_original,
        exploded_orders.order_total_discounted,
        (exploded_orders.order_total_original - exploded_orders.order_total_discounted).cast("decimal(10,2)").alias("order_total_discount"),
        exploded_orders.order_status,
        exploded_orders.payment_provider,
        exploded_orders.is_payment_done,
        exploded_orders.order_time,
        exploded_orders.date_key
    )
)

# Save Fact Table
(fact_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_orders"))

print("Fact table freeapi_gold_fact_orders generated successfully.")
