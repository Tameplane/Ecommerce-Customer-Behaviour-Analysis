# Power BI build guide

## Import
In Power BI Desktop choose **Get data > Text/CSV** and import each Gold CSV:
- gold_sales_daily
- gold_customer_summary
- gold_delivery_performance
- gold_payment_analysis
- gold_product_performance
- gold_sales_by_state

For each exported Spark CSV folder, use the single `part-....csv` file inside that folder. Rename each query to the matching Gold table name.

## Relationships
These are already aggregated Gold tables at different grains. Do not create arbitrary relationships between them. Build visuals from the relevant table, and use the supplied measures only where the underlying table supports them. For a simple first version, use table-specific visuals and slicers.

## Page 1 — Executive Overview
- Cards: Total Revenue, Total Orders, Average Order Value.
- Line chart: purchase_date vs total_revenue.
- Column chart: purchase_date vs total_orders (use month hierarchy).
- Bar chart: customer_state vs total_revenue (from gold_sales_by_state).

## Page 2 — Customer Insights
- Donut chart: customer_segment and distinct customer count.
- Bar chart: customer_state vs customer_revenue.
- Table: customer_unique_id, orders_count, customer_revenue, avg_review_score, last_purchase_date.

## Page 3 — Product & Payment
- Bar chart: category vs product_revenue (Top N 10).
- Column chart: category vs units_sold.
- Donut chart: payment_type vs payment_value.
- Card: total units sold.

## Page 4 — Delivery & Satisfaction
- Bar chart: delivery_status vs orders.
- Column chart: delivery_status vs avg_delivery_days.
- Column chart: delivery_status vs avg_review_score.

## Formatting
Use a clean light background, dark navy headings, one accent colour, consistent currency formatting labelled BRL, and clear chart titles. Add a footer: “Source: Olist Brazilian E-Commerce Public Dataset, 2016–2018”.
