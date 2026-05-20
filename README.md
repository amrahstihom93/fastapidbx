# FreeAPI Databricks Lakehouse Platform

This project implements a professional, 100% **Databricks-Native Lakehouse Platform** for analytical reporting on transactional data from the public `https://api.freeapi.app` servers (which refresh every 2 hours). It leverages **Auto Loader** and **Delta Lake** on **Databricks Community Edition (Free Tier)** to build a Medallion Architecture data pipeline.

---

## 🏗️ Architecture Overview

```
                      +--------------------------------------+
                      |      FreeAPI Public Web Server       |
                      +------------------+-------------------+
                                         |
                                         | (REST Ingestion every 1.5 hrs)
                                         v
                      +--------------------------------------+
                      |      DBFS / FileStore Landing        |
                      |   /FileStore/import/freeapi/raw/     |
                      +------------------+-------------------+
                                         |
                                         | (Auto Loader cloudFiles stream)
                                         v
                      +--------------------------------------+
                      |        Bronze Delta Schema           |
                      |       [catalog].[bronze].[table]     |
                      +------------------+-------------------+
                                         |
                                         | (Clean, Deduplicate & Cast)
                                         v
                      +--------------------------------------+
                      |        Silver Delta Schema           |
                      |       [catalog].[silver].[table]     |
                      +------------------+-------------------+
                                         |
                                         | (Star Schema joins)
                                         v
                      +--------------------------------------+
                      |         Gold Delta Schema            |
                      |        [catalog].[gold].[table]      |
                      +------------------+-------------------+
                                         |
                                         | (Validate Quality & Integrity)
                                         v
                      +--------------------------------------+
                      |     Data Quality Tests (Passed)      |
                      +------------------+-------------------+
                                         |
                                         | (OPTIMIZE / VACUUM 0 HOURS)
                                         v
                      +--------------------------------------+
                      |      Storage Cleanup & Reclaim       |
                      +--------------------------------------+
```

---

## 📁 Repository Structure

```
.
├── notebooks/
│   ├── 01_api_ingestion.py        # Ingests API data via Auto Loader stream
│   ├── 02_bronze_to_silver.py     # Deduplicates raw logs across server resets
│   ├── 03_silver_to_gold.py       # Star Schema (dim_products, dim_users, fact_orders)
│   ├── 04_data_quality.py         # DQ Assertions (referential & structural checks)
│   └── 05_delta_optimization.py   # Compactions & vacuum cleanups for free storage
├── .env.sample                    # Template for env variables
├── package.json                   # Project metadata
├── requirements.txt               # Local python dependency file
├── README.md                      # System deployment instructions
└── OPERATIONS.md                  # Dashboards & operational controls
```

---

## 🚀 Databricks Deployment Guide

### Step 1: Link Repository via Databricks Git Folders (Repos)
Instead of manually importing code, leverage Databricks' native Git integration:
1. Push this Git repository to a remote server (e.g. GitHub or GitLab).
2. Inside your Databricks workspace, click on **Git Folders** (formerly Repos) in the sidebar.
3. Click **Add Folder** -> **Create Git Folder**.
4. Enter the remote Git Repository URL and authenticate using your Personal Access Token (PAT).
5. Databricks will sync the repository structure natively. The Python source files in `notebooks/` will render directly as interactive Databricks notebooks.

### Step 2: Configure a Databricks Workflow (Job)
To automate execution and specify Unity Catalog naming conventions:
1. In Databricks, click on **Workflows** in the left menu.
2. Click **Create Job** (top right).
3. Configure the task sequence:
   * **Task 1: Ingest**
     * Type: `Notebook`
     * Source: `Git provider` (select your sync repository folder)
     * Path: `notebooks/01_api_ingestion`
     * Cluster: Select your running cluster.
     * Parameters: Add `catalog_name` parameter. Set to `hive_metastore` for Community Edition, or your Unity Catalog name (e.g., `freeapi_catalog`) for enterprise environments.
   * **Task 2: Silver** (Add task)
     * Type: `Notebook`
     * Path: `notebooks/02_bronze_to_silver`
     * Depends on: `Ingest`
     * Parameters: Add `catalog_name` parameter.
   * **Task 3: Gold** (Add task)
     * Type: `Notebook`
     * Path: `notebooks/03_silver_to_gold`
     * Depends on: `Silver`
     * Parameters: Add `catalog_name` parameter.
   * **Task 4: Data Quality** (Add task)
     * Type: `Notebook`
     * Path: `notebooks/04_data_quality`
     * Depends on: `Gold`
     * Parameters: Add `catalog_name` parameter.
   * **Task 5: Optimize** (Add task)
     * Type: `Notebook`
     * Path: `notebooks/05_delta_optimization`
     * Depends on: `Data Quality`
     * Parameters: Add `catalog_name` parameter.
4. Click **Create** to save the job definition. You now have an enterprise-ready, version-controlled DAG pipeline.

---

## 🛠️ Local Dry-Run Testing (Optional)

If you wish to test the API polling logic locally before running it on Databricks:
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up your `.env` file from the sample.
3. Run the verification script or run notebooks locally using a local spark instance.
