# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05: Delta Storage Optimization
# MAGIC This notebook optimizes and cleans up storage for all tables in the Delta Lakehouse.
# MAGIC Since this project is optimized for the **Databricks Community Edition (Free Tier)**, we must:
# MAGIC 1. **Compact small files** using `OPTIMIZE` to keep query speeds high on small drivers.
# MAGIC 2. **Purge obsolete file versions** using `VACUUM ... RETAIN 0 HOURS` to free up limited DBFS storage.

# COMMAND ----------

# Setup catalog widgets
dbutils.widgets.text("catalog_name", "hive_metastore", "Catalog Name (hive_metastore / freeapi_catalog)")
CATALOG = dbutils.widgets.get("catalog_name")

# Define namespaces
if CATALOG.lower() == "hive_metastore":
    BRONZE_SCHEMA = f"{CATALOG}.freeapi_bronze"
    SILVER_SCHEMA = f"{CATALOG}.freeapi_silver"
    GOLD_SCHEMA   = f"{CATALOG}.freeapi_gold"
else:
    BRONZE_SCHEMA = f"{CATALOG}.bronze"
    SILVER_SCHEMA = f"{CATALOG}.silver"
    GOLD_SCHEMA   = f"{CATALOG}.gold"

# Disable the 7-day (168 hours) vacuum retention safety check to allow immediate cleanup
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

# COMMAND ----------

# List of all Lakehouse tables to process
tables = [
    f"{BRONZE_SCHEMA}.users",
    f"{BRONZE_SCHEMA}.products",
    f"{BRONZE_SCHEMA}.orders",
    f"{SILVER_SCHEMA}.users",
    f"{SILVER_SCHEMA}.products",
    f"{SILVER_SCHEMA}.orders",
    f"{GOLD_SCHEMA}.dim_users",
    f"{GOLD_SCHEMA}.dim_products",
    f"{GOLD_SCHEMA}.dim_dates",
    f"{GOLD_SCHEMA}.fact_orders"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Execute Table Compactions & Vacuums

# COMMAND ----------

for table in tables:
    print(f"Optimizing table: {table}...")
    try:
        # Compact small files
        spark.sql(f"OPTIMIZE {table}")
        print(f" -> Compaction successful.")
        
        # Purge physical history and old transaction versions
        print(f"Vacuuming table: {table}...")
        spark.sql(f"VACUUM {table} RETAIN 0 HOURS")
        print(f" -> Vacuum successful. Storage reclaimed.")
        print("-"*40)
    except Exception as e:
        print(f" -> Failed to optimize/vacuum {table}. Error: {str(e)}")
        print("-"*40)

# COMMAND ----------

print("All Delta tables optimized and vacuumed successfully!")
