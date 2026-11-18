# Final Project Report — E-Commerce Customer Behaviour and Sales Intelligence

## 1. Introduction
E-commerce platforms generate transaction, customer, product, payment and delivery data. Analysing these datasets helps businesses understand sales performance, customer purchasing patterns, product demand and fulfilment quality.

## 2. Problem Statement
The project addresses the difficulty of converting multiple raw e-commerce datasets into reliable, decision-ready information. The solution builds a repeatable ETL pipeline and an interactive dashboard to support business analysis.

## 3. Objectives
- Integrate order, customer, product, payment and review data.
- Ingest source data into a Bronze layer.
- Clean, standardize and validate data in a Silver layer.
- Create Gold analytical tables using Apache Spark and Delta Lake.
- Visualize sales, customer, product and delivery metrics in Power BI.

## 4. Dataset and Sources
Primary source: Brazilian E-Commerce Public Dataset by Olist, published on Kaggle. It contains anonymized marketplace orders from 2016–2018 and related CSV tables. List the exact files used and cite the original Kaggle dataset URL. The tables represent multiple business domains but originate from the same publisher; describe them accurately.

## 5. Technology Stack
Python, PySpark, Apache Spark, Databricks Free Edition, Delta Lake, Power BI, Git and GitHub.

## 6. System Architecture
Insert the architecture diagram from `architecture.mmd`. Explain extraction, Bronze ingestion, Silver transformation, Gold aggregation and Power BI visualization.

## 7. Implementation
### 7.1 Data Extraction
Describe downloading the Olist public dataset and uploading the required CSV files to the Databricks Volume.

### 7.2 Bronze Layer
Describe raw CSV ingestion into Delta tables with minimal transformation.

### 7.3 Silver Layer
Describe timestamp and numeric type conversion, duplicate removal, null filtering and aggregation of one-to-many order items/payments/reviews before joining to prevent duplicated revenue.

### 7.4 Gold Layer
Describe the six dashboard-ready tables: daily sales, customer summary, delivery performance, payment analysis, product performance and sales by state.

### 7.5 Dashboard
Insert genuine screenshots of all Power BI pages and explain the business questions answered by each.

## 8. Data Quality and Validation
Record actual row counts, duplicate checks, null checks, key relationship checks and successful pipeline execution evidence. Only report checks that were actually run.

## 9. Results and Discussion
Insert actual figures from the completed Gold tables. Discuss revenue trend, order volume, average order value, repeat-customer share, top product categories, payment mix, late-delivery share and review scores. Do not invent values.

## 10. Limitations
The dataset is historical (2016–2018), anonymized and Brazil-specific. It does not represent current e-commerce conditions. The current implementation uses one public dataset publisher with multiple related tables; additional independent sources can be integrated if required by the instructor.

## 11. Conclusion
Summarize how the Spark and Delta Lake pipeline transformed raw data into Gold analytical tables and how Power BI makes the results accessible.

## 12. References
1. Olist, “Brazilian E-Commerce Public Dataset,” Kaggle: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Databricks Free Edition: https://www.databricks.com/learn/free-edition
3. Apache Spark documentation: https://spark.apache.org/docs/latest/
4. Delta Lake documentation: https://docs.delta.io/
