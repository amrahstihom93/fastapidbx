# FreeAPI Lakehouse Platform: Operations & Analytics Manual

This document guides the Data Platform and Analytics teams on operating the platform, monitoring data quality, building native dashboards, and running business queries using **Unity Catalog three-level namespaces**.

> [!NOTE]
> **Three-Level Namespace Note:**
> The SQL recipes below use the production Unity Catalog naming format: `[catalog].[schema].[table]`. 
> * **Production (Unity Catalog):** Replace `catalog_name` with your catalog (e.g., `freeapi_catalog`), yielding `freeapi_catalog.gold.fact_orders`.
> * **Development (Community Edition):** If using the local metastore, the path is mapped to the format `hive_metastore.freeapi_gold.fact_orders` (or `freeapi_silver`, `freeapi_bronze`).

---

## 📊 Databricks AI/BI Lakeview Dashboards

Databricks native **Lakeview Dashboards** are integrated into the workspace and run queries directly against your Gold tables.

### 1. Creating the Business Performance Dashboard
1. In your Databricks workspace, go to the sidebar and click **Dashboards** -> **Create Dashboard**.
2. Click **Add Widget** -> **Visualization**.
3. Under **Data**, select **Create query**. Write SQL queries to populate charts:
   * **KPI Card - Total Revenue**:
     ```sql
     SELECT sum(item_subtotal) as total_revenue 
     FROM freeapi_catalog.gold.fact_orders 
     WHERE order_status = 'DELIVERED';
     ```
   * **Chart - Daily Revenue Trend** (Line Chart):
     ```sql
     SELECT d.full_date, sum(f.item_subtotal) as daily_revenue
     FROM freeapi_catalog.gold.fact_orders f
     JOIN freeapi_catalog.gold.dim_dates d ON f.date_key = d.date_key
     GROUP BY d.full_date
     ORDER BY d.full_date;
     ```
   * **Chart - Revenue by Product Category** (Bar Chart):
     ```sql
     SELECT p.category_id, sum(f.item_subtotal) as category_revenue
     FROM freeapi_catalog.gold.fact_orders f
     JOIN freeapi_catalog.gold.dim_products p ON f.product_id = p.product_id
     GROUP BY p.category_id
     ORDER BY category_revenue DESC;
     ```
   * **KPI Card - Average Order Value (AOV)**:
     ```sql
     SELECT avg(order_total_discounted) as average_order_value
     FROM (
       SELECT DISTINCT order_id, order_total_discounted 
       FROM freeapi_catalog.gold.fact_orders
     );
     ```

### 2. Creating the Operations Monitor Dashboard
This tracks the health and speed of the pipeline:
* **Table Volume Tracking** (Bar Chart):
  ```sql
  SELECT 'Bronze Users' as table_name, count(*) as row_count FROM freeapi_catalog.bronze.users
  UNION ALL
  SELECT 'Bronze Products', count(*) FROM freeapi_catalog.bronze.products
  UNION ALL
  SELECT 'Bronze Orders', count(*) FROM freeapi_catalog.bronze.orders
  UNION ALL
  SELECT 'Gold Orders Fact', count(*) FROM freeapi_catalog.gold.fact_orders;
  ```
* **Failed Order Auditing**:
  ```sql
  SELECT order_id, user_email, order_status, order_time 
  FROM freeapi_catalog.gold.fact_orders 
  WHERE order_status = 'CANCELLED' 
  ORDER BY order_time DESC;
  ```

---

## 🔍 Troubleshooting Data Quality Failures

If the workflow fails at **Task 4: Data Quality**, it means one of the structural or business validation checks failed. 

To debug:
1. Open the failed run in the **Databricks Workflows Jobs UI**.
2. Click on the **Data Quality** task and inspect the standard output logs. You will see a test report indicating which test failed (e.g. `❌ Referential Integrity - fact_orders -> dim_products : FAILED`).
3. Open a Databricks SQL Editor or a new Scratch notebook, and run debug queries to locate the offending rows:

### Debugging PK Duplicates in Products:
```sql
SELECT product_id, count(*) 
FROM freeapi_catalog.gold.dim_products 
GROUP BY product_id 
HAVING count(*) > 1;
```

### Debugging Referential Integrity issues (Orphaned Orders):
If orders reference product IDs that do not exist in the product catalog:
```sql
SELECT f.order_id, f.product_id, f.order_time 
FROM freeapi_catalog.gold.fact_orders f
LEFT ANTI JOIN freeapi_catalog.gold.dim_products p ON f.product_id = p.product_id;
```

---

## 💡 Analytics Query Recipes

Analysts can run these recipes inside the **Databricks SQL Editor** to fetch reports:

### 1. Product Sales Performance
```sql
SELECT 
  p.name as product_name,
  sum(f.quantity) as total_units_sold,
  sum(f.item_subtotal) as total_revenue,
  avg(f.unit_price) as average_selling_price
FROM freeapi_catalog.gold.fact_orders f
JOIN freeapi_catalog.gold.dim_products p ON f.product_id = p.product_id
WHERE f.order_status = 'DELIVERED'
GROUP BY p.name
ORDER BY total_revenue DESC
LIMIT 10;
```

### 2. Coupon Discount Analysis
Analyze which coupons are generating the most discounts and order counts:
```sql
SELECT 
  coupon_id,
  count(distinct order_id) as total_orders,
  sum(order_total_discount) as total_discounts_granted,
  sum(order_total_discounted) as post_discount_sales
FROM freeapi_catalog.gold.fact_orders
WHERE coupon_id IS NOT NULL AND is_payment_done = true
GROUP BY coupon_id
ORDER BY total_discounts_granted DESC;
```

### 3. Shopping Behavior by Day of Week
See if users shop more on weekends:
```sql
SELECT 
  d.day_name,
  count(distinct f.order_id) as total_orders,
  sum(f.item_subtotal) as revenue,
  d.is_weekend
FROM freeapi_catalog.gold.fact_orders f
JOIN freeapi_catalog.gold.dim_dates d ON f.date_key = d.date_key
GROUP BY d.day_name, d.day_of_week, d.is_weekend
ORDER BY d.day_of_week;
```
