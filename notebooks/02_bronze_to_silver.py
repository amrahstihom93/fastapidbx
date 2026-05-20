# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Bronze to Silver - Data Cleaning & Deduplication
# MAGIC This notebook processes raw data from the Bronze tables:
# MAGIC 1. Parses nested JSON fields and normalizes schemas.
# MAGIC 2. Casts timestamp string values to Spark `TIMESTAMP` type.
# MAGIC 3. Performs robust deduplication using SQL Window functions to handle historical duplicates generated across multiple 2-hour server reset cycles.
# MAGIC 4. Saves the results as optimized Silver Delta tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Setup catalog widgets
dbutils.widgets.text("catalog_name", "hive_metastore", "Catalog Name (hive_metastore / freeapi_catalog)")
CATALOG = dbutils.widgets.get("catalog_name")

# Define namespaces
if CATALOG.lower() == "hive_metastore":
    BRONZE_SCHEMA = f"{CATALOG}.freeapi_bronze"
    SILVER_SCHEMA = f"{CATALOG}.freeapi_silver"
else:
    BRONZE_SCHEMA = f"{CATALOG}.bronze"
    SILVER_SCHEMA = f"{CATALOG}.silver"

print(f"Reading from Bronze Schema: {BRONZE_SCHEMA}")
print(f"Writing to Silver Schema: {SILVER_SCHEMA}")

# Ensure Silver schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Deduplicate and Clean Users
# MAGIC * Natural Key: `email`
# MAGIC * Action: Deduplicate users by email, keeping the most recently ingested record.

# COMMAND ----------

print("Processing Users (Bronze -> Silver)...")

# Read bronze users
users_df = spark.table(f"{BRONZE_SCHEMA}.users")

# Deduplicate window: partition by email, order by ingest time descending
user_window = Window.partitionBy("email").orderBy(F.col("_ingest_time").desc())

cleaned_users_df = (users_df
    .withColumn("rn", F.row_number().over(user_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("username").alias("username"),
        F.col("email").alias("email"),
        F.col("role").alias("role"),
        F.to_timestamp(F.col("createdAt")).alias("created_at"),
        F.to_timestamp(F.col("updatedAt")).alias("updated_at"),
        F.col("_ingest_time").alias("ingest_time")
    )
)

# Write to Silver Delta Table
(cleaned_users_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.users"))

print(f"Silver Users table successfully updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deduplicate and Clean Products
# MAGIC * Natural Key: `name`
# MAGIC * Action: Parse nested `mainImage` sub-document, cast dates, and deduplicate by product name.

# COMMAND ----------

print("Processing Products (Bronze -> Silver)...")

# Read bronze products
products_df = spark.table(f"{BRONZE_SCHEMA}.products")

# Deduplicate window: partition by product name, order by ingest time descending
product_window = Window.partitionBy("name").orderBy(F.col("_ingest_time").desc())

cleaned_products_df = (products_df
    .withColumn("rn", F.row_number().over(product_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("_id").alias("product_id"),
        F.col("name").alias("name"),
        F.col("description").alias("description"),
        F.col("price").cast("decimal(10,2)").alias("price"),
        F.col("stock").cast("integer").alias("stock"),
        F.col("category").alias("category_id"),
        F.col("mainImage.url").alias("main_image_url"),
        F.to_timestamp(F.col("createdAt")).alias("created_at"),
        F.to_timestamp(F.col("updatedAt")).alias("updated_at"),
        F.col("_ingest_time").alias("ingest_time")
    )
)

# Write to Silver Delta Table
(cleaned_products_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.products"))

print(f"Silver Products table successfully updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Deduplicate and Clean Orders
# MAGIC * Natural Key: `_id` (Mongo order ID)
# MAGIC * Action: Extract nested elements, parse payment details, cast pricing, and deduplicate by order ID.

# COMMAND ----------

print("Processing Orders (Bronze -> Silver)...")

# Read bronze orders
orders_df = spark.table(f"{BRONZE_SCHEMA}.orders")

# Deduplicate window: partition by order _id, order by ingest time descending
order_window = Window.partitionBy("_id").orderBy(F.col("_ingest_time").desc())

cleaned_orders_df = (orders_df
    .withColumn("rn", F.row_number().over(order_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("_id").alias("order_id"),
        F.col("customer").alias("customer_id"),
        F.col("addressId").alias("address_id"),
        F.col("orderStatus").alias("order_status"),
        F.col("paymentProvider").alias("payment_provider"),
        F.col("paymentId").alias("payment_id"),
        F.col("isPaymentDone").cast("boolean").alias("is_payment_done"),
        F.col("orderPrice").cast("decimal(10,2)").alias("original_price"),
        F.col("discountedPrice").cast("decimal(10,2)").alias("discounted_price"),
        F.col("coupon").alias("coupon_id"),
        F.col("items").alias("order_items"), # Array of structs [{productId: string, quantity: int}]
        F.to_timestamp(F.col("createdAt")).alias("created_at"),
        F.to_timestamp(F.col("updatedAt")).alias("updated_at"),
        F.col("_ingest_time").alias("ingest_time")
    )
)

# Write to Silver Delta Table
(cleaned_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.orders"))

print(f"Silver Orders table successfully updated.")
