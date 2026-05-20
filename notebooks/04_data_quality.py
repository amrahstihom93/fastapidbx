# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04: Data Quality Assertion Framework
# MAGIC This notebook runs critical data quality tests on our Delta tables. If any test fails, it raises an exception, which will fail the Databricks Workflow task and stop the pipeline from continuing.
# MAGIC 
# MAGIC Assertions checked:
# MAGIC 1. **Primary Key Uniqueness** on dimensions.
# MAGIC 2. **Null Constraints** on critical ID and metric columns.
# MAGIC 3. **Value Bound Checks** (e.g. quantities must be > 0).
# MAGIC 4. **Referential Integrity** (Foreign Key checks linking fact to dimensions).

# COMMAND ----------

# Setup catalog widgets
dbutils.widgets.text("catalog_name", "hive_metastore", "Catalog Name (hive_metastore / freeapi_catalog)")
CATALOG = dbutils.widgets.get("catalog_name")

# Define namespaces
if CATALOG.lower() == "hive_metastore":
    GOLD_SCHEMA   = f"{CATALOG}.freeapi_gold"
else:
    GOLD_SCHEMA   = f"{CATALOG}.gold"

print(f"Asserting Data Quality on Gold Schema: {GOLD_SCHEMA}")

# Initialize report summary list
dq_report = []
any_failure = False

def run_test(test_name, query_df, assertion_fn):
    global any_failure
    print(f"Running: {test_name}...")
    try:
        # Evaluate assertion
        result = assertion_fn(query_df)
        if result:
            dq_report.append((test_name, "PASSED", "All constraints satisfied."))
            print(f" -> PASSED")
        else:
            dq_report.append((test_name, "FAILED", "Assertion check failed on dataset!"))
            print(f" -> FAILED ❌")
            any_failure = True
    except Exception as e:
        dq_report.append((test_name, "ERROR", str(e)))
        print(f" -> ERROR ❌: {str(e)}")
        any_failure = True

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Primary Key Uniqueness Checks

# COMMAND ----------

# Unique Users test
users_df = spark.table(f"{GOLD_SCHEMA}.dim_users")
run_test(
    "PK Uniqueness - dim_users (email)",
    users_df,
    lambda df: df.groupBy("email").count().filter("count > 1").count() == 0
)

# Unique Products test
products_df = spark.table(f"{GOLD_SCHEMA}.dim_products")
run_test(
    "PK Uniqueness - dim_products (product_id)",
    products_df,
    lambda df: df.groupBy("product_id").count().filter("count > 1").count() == 0
)

# Unique Dates test
dates_df = spark.table(f"{GOLD_SCHEMA}.dim_dates")
run_test(
    "PK Uniqueness - dim_dates (date_key)",
    dates_df,
    lambda df: df.groupBy("date_key").count().filter("count > 1").count() == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Null Value Checks

# COMMAND ----------

# Orders null check
orders_fact = spark.table(f"{GOLD_SCHEMA}.fact_orders")
run_test(
    "Null Check - fact_orders (critical columns)",
    orders_fact,
    lambda df: df.filter(
        df.order_id.isNull() | 
        df.user_email.isNull() | 
        df.product_id.isNull() | 
        df.date_key.isNull()
    ).count() == 0
)

# Users name/role null check
run_test(
    "Null Check - dim_users (critical columns)",
    users_df,
    lambda df: df.filter(df.email.isNull() | df.username.isNull() | df.role.isNull()).count() == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Business Value Bound Checks

# COMMAND ----------

# Quantity bound checks
run_test(
    "Bound Check - fact_orders (quantity > 0)",
    orders_fact,
    lambda df: df.filter(df.quantity <= 0).count() == 0
)

# Price bound checks
run_test(
    "Bound Check - fact_orders (prices >= 0)",
    orders_fact,
    lambda df: df.filter((df.unit_price < 0) | (df.item_subtotal < 0)).count() == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Referential Integrity Checks

# COMMAND ----------

# User FK check
run_test(
    "Referential Integrity - fact_orders -> dim_users",
    orders_fact,
    lambda df: df.join(users_df, df.user_email == users_df.email, "left_anti").count() == 0
)

# Product FK check
run_test(
    "Referential Integrity - fact_orders -> dim_products",
    orders_fact,
    lambda df: df.join(products_df, df.product_id == products_df.product_id, "left_anti").count() == 0
)

# Date FK check
run_test(
    "Referential Integrity - fact_orders -> dim_dates",
    orders_fact,
    lambda df: df.join(dates_df, df.date_key == dates_df.date_key, "left_anti").count() == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Compile and Output Report

# COMMAND ----------

print("\n" + "="*50)
print("             DATA QUALITY TEST REPORT")
print("="*50)
for name, status, details in dq_report:
    icon = "✅" if status == "PASSED" else "❌"
    print(f"{icon} {name:<45} : {status:<8} - {details}")
print("="*50)

if any_failure:
    raise Exception("Data Quality tests failed! Halting pipeline execution to prevent bad data load.")
else:
    print("All Data Quality checks passed successfully!")
    # Save a verification report in DBFS for audit logging
    dbutils.notebook.exit("SUCCESS")
