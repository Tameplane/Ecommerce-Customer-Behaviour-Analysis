# Databricks notebook source
# E-Commerce Customer Behaviour and Sales Intelligence
# Dataset: Brazilian E-Commerce Public Dataset by Olist (Kaggle)
# Upload the 9 CSV files into /Volumes/workspace/default/ecommerce_raw/ before running.
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_PATH = "/Volumes/workspace/default/ecommerce_raw"
CATALOG = "workspace"
SCHEMA = "default"

# COMMAND ----------
# Create managed Delta tables in the default schema.
# Bronze: raw source data with minimal changes.
files = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

for table_name, file_name in files.items():
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(f"{RAW_PATH}/{file_name}"))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{table_name}"))

# COMMAND ----------
# Quick Bronze validation
for table_name in files:
    print(table_name, spark.table(f"{CATALOG}.{SCHEMA}.bronze_{table_name}").count())

# COMMAND ----------
# SILVER: standardize fields, parse timestamps, remove duplicate keys,
# and filter records that cannot be used in the relevant analysis.
orders = spark.table("workspace.default.bronze_orders")
customers = spark.table("workspace.default.bronze_customers")
items = spark.table("workspace.default.bronze_order_items")
payments = spark.table("workspace.default.bronze_order_payments")
reviews = spark.table("workspace.default.bronze_order_reviews")
products = spark.table("workspace.default.bronze_products")
sellers = spark.table("workspace.default.bronze_sellers")
translation = spark.table("workspace.default.bronze_category_translation")

silver_orders = (
    orders.dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
    .filter(F.col("order_id").isNotNull())
)

silver_customers = customers.dropDuplicates(["customer_id"]).filter(F.col("customer_id").isNotNull())
silver_items = (
    items.dropDuplicates(["order_id", "order_item_id"])
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .filter(F.col("order_id").isNotNull() & F.col("price").isNotNull())
)
silver_payments = (
    payments.withColumn("payment_value", F.col("payment_value").cast("double"))
    .filter(F.col("order_id").isNotNull() & F.col("payment_value").isNotNull())
)
silver_reviews = (
    reviews.dropDuplicates(["review_id"])
    .withColumn("review_score", F.col("review_score").cast("int"))
    .filter(F.col("order_id").isNotNull())
)
silver_products = products.dropDuplicates(["product_id"])
silver_sellers = sellers.dropDuplicates(["seller_id"])
silver_translation = translation.dropDuplicates(["product_category_name"])

silver_frames = {
    "orders": silver_orders, "customers": silver_customers,
    "order_items": silver_items, "order_payments": silver_payments,
    "order_reviews": silver_reviews, "products": silver_products,
    "sellers": silver_sellers, "category_translation": silver_translation
}
for name, df in silver_frames.items():
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"workspace.default.silver_{name}"))

# COMMAND ----------
# SILVER: build order-level facts. Aggregate each one-to-many table BEFORE joining
# to avoid multiplying order revenue by payment/review/item rows.
item_by_order = (
    silver_items.groupBy("order_id")
    .agg(
        F.sum("price").alias("item_revenue"),
        F.sum("freight_value").alias("freight_revenue"),
        F.count("*").alias("item_count")
    )
)
payment_by_order = (
    silver_payments.groupBy("order_id")
    .agg(F.sum("payment_value").alias("payment_total"),
         F.countDistinct("payment_type").alias("payment_type_count"))
)
review_by_order = (
    silver_reviews.groupBy("order_id")
    .agg(F.avg("review_score").alias("average_review_score"))
)

order_fact = (
    silver_orders.join(silver_customers, "customer_id", "left")
    .join(item_by_order, "order_id", "left")
    .join(payment_by_order, "order_id", "left")
    .join(review_by_order, "order_id", "left")
    .withColumn("total_sales_value", F.coalesce(F.col("item_revenue"), F.lit(0.0)) +
                                    F.coalesce(F.col("freight_revenue"), F.lit(0.0)))
    .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
    .withColumn("delivery_days",
        F.when(F.col("order_delivered_customer_date").isNotNull(),
               F.datediff(F.to_date("order_delivered_customer_date"),
                          F.to_date("order_purchase_timestamp"))))
    .withColumn("delivery_status",
        F.when(F.col("order_delivered_customer_date").isNull(), "Not Delivered")
         .when(F.to_date("order_delivered_customer_date") <=
               F.to_date("order_estimated_delivery_date"), "On Time")
         .otherwise("Late"))
)
(order_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("workspace.default.silver_order_fact"))

# COMMAND ----------
# GOLD: business-ready aggregate tables for dashboarding.
fact = spark.table("workspace.default.silver_order_fact")

gold_sales_daily = (
    fact.groupBy("purchase_date")
    .agg(F.sum("total_sales_value").alias("total_revenue"),
         F.countDistinct("order_id").alias("total_orders"),
         F.avg("total_sales_value").alias("average_order_value"))
    .orderBy("purchase_date")
)
gold_customer_summary = (
    fact.groupBy("customer_unique_id", "customer_state", "customer_city")
    .agg(F.countDistinct("order_id").alias("orders_count"),
         F.sum("total_sales_value").alias("customer_revenue"),
         F.avg("average_review_score").alias("avg_review_score"),
         F.max("purchase_date").alias("last_purchase_date"))
    .withColumn("customer_segment",
        F.when(F.col("orders_count") >= 2, "Repeat Customer").otherwise("One-time Customer"))
)
gold_delivery_performance = (
    fact.groupBy("delivery_status")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.avg("delivery_days").alias("avg_delivery_days"),
         F.avg("average_review_score").alias("avg_review_score"))
)
gold_payment_analysis = (
    silver_payments.groupBy("payment_type")
    .agg(F.count("*").alias("payment_transactions"),
         F.sum("payment_value").alias("payment_value"),
         F.avg("payment_installments").alias("avg_installments"))
)
item_category = (
    silver_items.join(silver_products, "product_id", "left")
    .join(silver_translation, "product_category_name", "left")
    .withColumn("category",
        F.coalesce(F.col("product_category_name_english"),
                   F.col("product_category_name"), F.lit("Unknown")))
)
gold_product_performance = (
    item_category.groupBy("category")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.sum("price").alias("product_revenue"),
         F.sum("freight_value").alias("freight_revenue"),
         F.count("*").alias("units_sold"))
    .orderBy(F.desc("product_revenue"))
)
gold_sales_by_state = (
    fact.groupBy("customer_state")
    .agg(F.countDistinct("order_id").alias("orders"),
         F.sum("total_sales_value").alias("total_revenue"),
         F.countDistinct("customer_unique_id").alias("unique_customers"))
    .orderBy(F.desc("total_revenue"))
)

gold_tables = {
    "gold_sales_daily": gold_sales_daily,
    "gold_customer_summary": gold_customer_summary,
    "gold_delivery_performance": gold_delivery_performance,
    "gold_payment_analysis": gold_payment_analysis,
    "gold_product_performance": gold_product_performance,
    "gold_sales_by_state": gold_sales_by_state
}
for name, df in gold_tables.items():
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"workspace.default.{name}"))

# COMMAND ----------
# Final checks: these outputs should be captured as screenshots for the report.
for name in gold_tables:
    print(f"\n{name}")
    spark.table(f"workspace.default.{name}").show(10, truncate=False)

# COMMAND ----------
# Export Gold tables to CSV for Power BI Desktop.
# Files are written to the Databricks Volume; download them from the Data Explorer.
EXPORT_PATH = "/Volumes/workspace/default/ecommerce_gold_export"
for name in gold_tables:
    (spark.table(f"workspace.default.{name}").coalesce(1)
     .write.mode("overwrite").option("header", True)
     .csv(f"{EXPORT_PATH}/{name}"))
print("Gold exports completed:", EXPORT_PATH)
