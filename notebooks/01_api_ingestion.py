# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: FreeAPI API Ingestion & Auto Loader
# MAGIC This notebook is the first step of the Lakehouse pipeline. It:
# MAGIC 1. Fetches transactional data (Users, Products, Orders) from the public FreeAPI server (`api.freeapi.app`).
# MAGIC 2. Automatically seeds the remote database if it is found unseeded.
# MAGIC 3. Authenticates as an Admin user to retrieve admin-restricted order data.
# MAGIC 4. Saves the raw payloads as JSON files in DBFS landing zone.
# MAGIC 5. Uses **Databricks Auto Loader** to stream the landing files into Delta Bronze tables and automatically deletes processed raw JSON files.

# COMMAND ----------

import os
import json
import time
import requests
from datetime import datetime

# COMMAND ----------

# Setup notebook widgets for parameterization
dbutils.widgets.text("catalog_name", "hive_metastore", "Catalog Name (hive_metastore / freeapi_catalog)")
dbutils.widgets.text("base_url", "https://api.freeapi.app/api/v1", "FreeAPI Base URL")
dbutils.widgets.text("landing_base_path", "/FileStore/import/freeapi", "DBFS Base Path")

CATALOG = dbutils.widgets.get("catalog_name")
BASE_URL = dbutils.widgets.get("base_url")
LANDING_BASE_PATH = dbutils.widgets.get("landing_base_path")

# Define namespaces
if CATALOG.lower() == "hive_metastore":
    BRONZE_SCHEMA = f"{CATALOG}.freeapi_bronze"
else:
    BRONZE_SCHEMA = f"{CATALOG}.bronze"

print(f"Configured Catalog: {CATALOG}")
print(f"Configured Schema: {BRONZE_SCHEMA}")
print(f"Configured Base URL: {BASE_URL}")
print(f"Configured Landing Path: {LANDING_BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Self-Healing Authentication & Seed Logic

# COMMAND ----------

def ensure_seeded_and_get_admin_token():
    print("Checking if database contains seeded credentials...")
    cred_url = f"{BASE_URL}/seed/generated-credentials"
    users = []
    
    try:
        response = requests.get(cred_url, timeout=30)
        if response.status_code == 200:
            users = response.json().get("data", [])
            print(f"Credentials file found with {len(users)} users.")
        else:
            print("Credentials file not found. Database needs seeding.")
    except Exception as e:
        print(f"Failed to check credentials file: {e}")
        
    if not users:
        print("Database is unseeded. Seeding ecommerce database now...")
        seed_res = requests.post(f"{BASE_URL}/seed/ecommerce", timeout=60)
        if seed_res.status_code in [200, 201]:
            print("Seeding successful! Fetching credentials...")
            time.sleep(3) # Wait for Mongo indices
            response = requests.get(cred_url, timeout=30)
            response.raise_for_status()
            users = response.json().get("data", [])
        else:
            raise Exception(f"Failed to seed database: {seed_res.text}")

    # Find Admin credentials
    admin_user = next((u for u in users if u.get("role") == "ADMIN"), None)
    if not admin_user:
        raise Exception("No user with role ADMIN found in credentials.")
    
    # Save users raw list for user ingestion
    temp_users_path = "/tmp/raw_users.json"
    with open(temp_users_path, "w") as f:
        json.dump(users, f)
        
    print(f"Attempting login for Admin User: {admin_user['username']}")
    
    # Login and retrieve JWT Token
    login_url = f"{BASE_URL}/users/login"
    login_payload = {
        "username": admin_user["username"],
        "password": admin_user["password"]
    }
    
    login_res = requests.post(login_url, json=login_payload, timeout=30)
    
    # Self-healing: if login returns 404 (stale credentials file after DB wipe), trigger re-seeding!
    if login_res.status_code == 404:
        print("Stale credentials file found (User does not exist in DB). Re-seeding database...")
        seed_res = requests.post(f"{BASE_URL}/seed/ecommerce", timeout=60)
        if seed_res.status_code in [200, 201]:
            print("Re-seeding successful! Fetching credentials...")
            time.sleep(3)
            response = requests.get(cred_url, timeout=30)
            response.raise_for_status()
            users = response.json().get("data", [])
            
            # Save fresh users list
            with open(temp_users_path, "w") as f:
                json.dump(users, f)
                
            admin_user = next((u for u in users if u.get("role") == "ADMIN"), None)
            if not admin_user:
                raise Exception("No ADMIN user found in fresh credentials.")
                
            login_payload = {
                "username": admin_user["username"],
                "password": admin_user["password"]
            }
            print(f"Logging in with fresh credentials: {admin_user['username']}")
            login_res = requests.post(login_url, json=login_payload, timeout=30)
        else:
            raise Exception(f"Failed to re-seed database: {seed_res.text}")
            
    login_res.raise_for_status()
    token = login_res.json().get("data", {}).get("accessToken")
    if not token:
        raise Exception("Access token missing in login response.")
    
    print("Admin JWT authentication successful!")
    return token, temp_users_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Fetch Products & Orders, Save to Local Temp Path

# COMMAND ----------

def fetch_and_stage_raw_data(admin_token):
    headers = {"Authorization": f"Bearer {admin_token}"}
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    
    # 1. Fetch Products (Public endpoint)
    print("Fetching products...")
    prod_url = f"{BASE_URL}/ecommerce/products?page=1&limit=100"
    prod_res = requests.get(prod_url, timeout=30)
    prod_res.raise_for_status()
    products = prod_res.json().get("data", {}).get("products", [])
    
    temp_products_path = f"/tmp/products_{timestamp}.json"
    with open(temp_products_path, "w") as f:
        json.dump(products, f)
    print(f"Staged {len(products)} products to local temp.")
    
    # 2. Fetch Orders (Admin endpoint)
    print("Fetching orders list (Admin API)...")
    order_url = f"{BASE_URL}/ecommerce/orders/list/admin?page=1&limit=100"
    order_res = requests.get(order_url, headers=headers, timeout=30)
    order_res.raise_for_status()
    orders = order_res.json().get("data", {}).get("orders", [])
    
    temp_orders_path = f"/tmp/orders_{timestamp}.json"
    with open(temp_orders_path, "w") as f:
        json.dump(orders, f)
    print(f"Staged {len(orders)} orders to local temp.")
    
    return temp_products_path, temp_orders_path, timestamp

# COMMAND ----------

# Run the seeding, auth and API extraction
token, temp_users_file = ensure_seeded_and_get_admin_token()
temp_products_file, temp_orders_file, run_ts = fetch_and_stage_raw_data(token)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Copy Staged JSON Payloads to DBFS Landing Zone

# COMMAND ----------

# Define landing directory paths
users_landing = f"{LANDING_BASE_PATH}/raw/users"
products_landing = f"{LANDING_BASE_PATH}/raw/products"
orders_landing = f"{LANDING_BASE_PATH}/raw/orders"

# Ensure target landing directories exist
dbutils.fs.mkdirs(users_landing)
dbutils.fs.mkdirs(products_landing)
dbutils.fs.mkdirs(orders_landing)

# Move staged files from local cluster /tmp to DBFS
dbutils.fs.mv(f"file:{temp_users_file}", f"{users_landing}/users_{run_ts}.json")
dbutils.fs.mv(f"file:{temp_products_file}", f"{products_landing}/products_{run_ts}.json")
dbutils.fs.mv(f"file:{temp_orders_file}", f"{orders_landing}/orders_{run_ts}.json")

print("Files successfully transferred to DBFS landing zone:")
print(f" - Users: {users_landing}/users_{run_ts}.json")
print(f" - Products: {products_landing}/products_{run_ts}.json")
print(f" - Orders: {orders_landing}/orders_{run_ts}.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Auto Loader Streaming to Bronze Delta Tables

# COMMAND ----------

def run_autoloader(entity_name, landing_dir, target_table):
    checkpoint_dir = f"{LANDING_BASE_PATH}/checkpoints/{entity_name}"
    schema_dir = f"{LANDING_BASE_PATH}/schemas/{entity_name}"
    
    print(f"Starting Auto Loader stream for: {entity_name}")
    print(f"Landing Dir: {landing_dir}")
    print(f"Target Delta Table: {target_table}")
    
    # Read stream using Auto Loader (cloudFiles)
    stream_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_dir)
        .option("cloudFiles.inferColumnTypes", "true")
        # Optimization: Delete source files after successful load to save storage
        .option("cloudFiles.cleanSource", "delete") 
        .load(landing_dir))
    
    # Add metadata column for traceability
    from pyspark.sql.functions import current_timestamp, input_file_name
    enriched_df = (stream_df
        .withColumn("_ingest_time", current_timestamp())
        .withColumn("_input_file", input_file_name()))
    
    # Write Stream using AvailableNow Trigger (batch-style execution)
    query = (enriched_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .toTable(target_table))
    
    # Wait for the stream to complete execution
    query.awaitTermination()
    print(f"Completed Auto Loader stream for {entity_name}.")

# COMMAND ----------

# Create Bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# Run Auto Loader for Users, Products, and Orders
run_autoloader("users", users_landing, f"{BRONZE_SCHEMA}.users")
run_autoloader("products", products_landing, f"{BRONZE_SCHEMA}.products")
run_autoloader("orders", orders_landing, f"{BRONZE_SCHEMA}.orders")
