# E-Commerce Customer Behaviour and Sales Intelligence

## Project overview
An end-to-end data engineering and analytics project using the Brazilian E-Commerce Public Dataset by Olist. It studies sales, customer behaviour, product demand, payment patterns and delivery performance.

**Required stack:** Python, Apache Spark (PySpark), Databricks, Delta Lake, Power BI.

## Dataset
Download the **Brazilian E-Commerce Public Dataset by Olist** from:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The dataset contains anonymized Brazilian marketplace orders from 2016–2018 and nine CSV files. The pipeline uses eight core files; the large geolocation file is optional for the core dashboard.

Required CSV files:
- olist_orders_dataset.csv
- olist_customers_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv

**Source note:** These are multiple related public dataset tables from one publisher (Olist), not three independent publishers. If your instructor interprets “three data sources” strictly as three independent providers, ask whether the Olist companion Marketing Funnel dataset and a public holiday/calendar source should be added as separate sources. Do not claim they are integrated until they actually are.

## Databricks setup
1. Create a free Databricks account: https://www.databricks.com/learn/free-edition
2. Open the workspace and create a Unity Catalog Volume named `ecommerce_raw` in catalog `workspace`, schema `default`.
3. Upload the eight required CSV files into that Volume. The resulting paths must be:
   `/Volumes/workspace/default/ecommerce_raw/<filename>`
4. Import `notebooks/ecommerce_medallion_pipeline.py` as a Databricks notebook.
5. Run all cells from top to bottom. The notebook uses the existing serverless Spark session; do not create a cluster manually.
6. Verify that Bronze, Silver and Gold tables were created under `workspace.default`.
7. Download the CSV files from `/Volumes/workspace/default/ecommerce_gold_export/` for Power BI.

If your workspace shows a different catalog/schema or does not allow creating a Volume, stop and share a screenshot before changing the notebook paths.

## Medallion architecture
- Bronze: source CSVs ingested as Delta tables with minimal changes.
- Silver: typed timestamps/numeric fields, duplicate removal, null/key checks and order-level joins.
- Gold: business-ready aggregates for sales, customers, delivery, payments, product categories and states.

## Power BI dashboard
Import the six Gold CSV exports from `powerbi/` instructions. Recommended report pages:
1. Executive Overview: revenue, orders, average order value, monthly revenue.
2. Customer Insights: one-time vs repeat customers, customer revenue, state distribution.
3. Product & Payment: top categories, units sold, payment mix.
4. Delivery & Satisfaction: on-time/late/not-delivered orders, delivery duration, review score.

Use the supplied DAX measures in `powerbi/measures.dax`.

## Important analytical definitions
- Total sales value = item price + freight value.
- Average order value = total sales value / distinct orders.
- Repeat customer = customer_unique_id with at least two orders in the dataset.
- Late delivery = actual customer delivery date later than estimated delivery date.
- The dataset is historical (2016–2018), so dashboard findings describe that period, not current market conditions.
- The dataset is Brazilian; monetary values are in Brazilian real (BRL), not INR.

## Repository layout
- `notebooks/`: Databricks PySpark pipeline.
- `powerbi/`: dashboard design and DAX measures.
- `docs/`: report outline, presentation outline and architecture.
- `data/raw/`: place downloaded source CSVs here locally; do not commit large raw files to GitHub.

## Evidence to capture
Take genuine screenshots of:
1. Databricks Bronze tables and row counts.
2. Silver order fact table preview.
3. Gold table previews.
4. Successful notebook run.
5. Power BI dashboard pages.

Do not use fabricated screenshots or claim a run succeeded until it has run successfully.
