# medallionarchitecture

Bronze (Raw)
Raw data ingestion — typically messy, unprocessed.

Silver (Cleaned)
Cleaned and enriched data — deduplicated, joined, formatted.

Gold (Business)
Aggregated data for business consumption — dashboards, ML models, KPIs.


✅ Summary of Operations per Layer
Layer	Files	Operations Applied
Bronze	customers_bronze.csv, orders_bronze.csv	Raw data ingestion
Silver	orders_silver.csv, customer_orders_silver.csv	Filtering, joins, formatting
Gold	customer_cltv_gold.csv	Aggregation: total spend, order count


/bronze/
    customers/
    orders/
/silver/
    customers/
    orders/
    customer_orders/
/gold/
    customer_cltv/
