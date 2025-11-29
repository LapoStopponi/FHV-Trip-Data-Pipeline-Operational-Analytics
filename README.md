# FHV-Trip-Data-Pipeline-Operational-Analytics
In this project, I implemented an end-to-end Data Lakehouse ETL pipeline (Bronze, Silver, Gold) using the Databricks platform.
The primary goal of the project was to process a large and real world transportation dataset (1.33 GB) to derive actionable
business intelligence, demonstrating proficiency in data engineering fundamentals, data reliability, and advanced analytical SQL.

KEY SKILLS DEMONSTRATED
- Data Engineering fundamentals: Bypassed cloud environment limitations by leveraging cloud storage (GCS) and automated ingestion (Fivetran)
- Scalability and Processing: Handled a large-scale dataset (23M+ records) efficiently using PySpark and Delta Lake
- Data Reliability: Implemented rigorous data quality checks (Layer Silver)
- Advanced Analytics: Used Windows functions (LAG, RANK) and CTEs in SQL to calculate key operational metrics.

DATA SOURCE AND ARCHITECTURE
Data Source:
- Dataset: NYC FHV (For-Hire Vehicle) Trip for January 2019
- Volume: 1.33 GB (23.143.222 records)
- Ingestion Method: Google Cloud Storage (GCS) -> Fivetran -> Databricks

DATA LAKEHOUSE ARCHITECTURE
The pipeline follows the industry-standard Medallion Architecture:
- Bronze Layer (Raw): Stores the raw, exact copy of the data ingested via Fivetran from GCS.
    - Table: workspace.fhv_data_pipeline.raw_fhv_trips
- Silver Layer (Cleaned): Contains cleaned, filtered, and type-converted data, ready for analysis.
    - Table: silver_clean_fhv_data
- Gold Layer (Aggregated/Business Metrics): Stores final aggregated metrics and calculated features used for reporting and business decisions.
    - Table: gold_base_performance

PIPELINE IMPLEMENTATION STEPS
1) Silver Layer: Data Quality and Transformation (PySpark)
   With this step, I cleansed the raw data and transformed the key fields. The code performs filtering for data reliability.
   Key Operations:
   - Type Casting: I renamed and utilized existing timestamp columns(tpep_pickup_datetime, tpwp_dropoff_datetime)
   - Filtering: I removed records with null or zero location IDs (pulocation_id, dolocation_id) and temporal inconsistencies (where droppoff time
     was before pickup time)
```python
# Rename existing, correctly typed timestamp columns to standard analysis names.
df_silver = df_bronze.withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
                     .withColumnRenamed("drop_off_datetime", "tpep_dropoff_datetime") 
```
