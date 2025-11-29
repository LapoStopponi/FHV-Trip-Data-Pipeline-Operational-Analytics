# FHV-Trip-Data-Pipeline-Operational-Analytics
In this project, I implemented an end-to-end Data Lakehouse ETL pipeline (Bronze, Silver, Gold) using the Databricks platform.
The primary goal of the project was to process a large and real world transportation dataset (1.33 GB) to derive actionable
business intelligence, demonstrating proficiency in data engineering fundamentals, data reliability, and advanced analytical SQL.

**KEY SKILLS DEMONSTRATED**
- Data Engineering fundamentals: Bypassed cloud environment limitations by leveraging cloud storage (GCS) and automated ingestion (Fivetran)
- Scalability and Processing: Handled a large-scale dataset (23M+ records) efficiently using PySpark and Delta Lake
- Data Reliability: Implemented rigorous data quality checks (Layer Silver)
- Advanced Analytics: Used Windows functions (LAG, RANK) and CTEs in SQL to calculate key operational metrics.

**DATA SOURCE AND ARCHITECTURE**

**Data Source:**
- Dataset: NYC FHV (For-Hire Vehicle) Trip for January 2019
- Volume: 1.33 GB (23.143.222 records)
- Ingestion Method: Google Cloud Storage (GCS) -> Fivetran -> Databricks

**DATA LAKEHOUSE ARCHITECTURE**

The pipeline follows the industry-standard Medallion Architecture:
- **Bronze Layer (Raw)**: Stores the raw, exact copy of the data ingested via Fivetran from GCS.
    - Table: workspace.fhv_data_pipeline.raw_fhv_trips
- **Silver Layer (Cleaned)**: Contains cleaned, filtered, and type-converted data, ready for analysis.
    - Table: silver_clean_fhv_data
- **Gold Layer (Aggregated/Business Metrics)**: Stores final aggregated metrics and calculated features used for reporting and business decisions.
    - Table: gold_base_performance

*PIPELINE IMPLEMENTATION STEPS*

1. **Silver Layer: Data Quality and Transformation (PySpark)**

   With this step, I cleansed the raw data and transformed the key fields. The code performs filtering for data reliability.
   Key Operations:
   - **Type Casting0:** I renamed and utilized existing timestamp columns(tpep_pickup_datetime, tpwp_dropoff_datetime)
   - **Filtering:** I removed records with null or zero location IDs (pulocation_id, dolocation_id) and temporal inconsistencies (where droppoff time
     was before pickup time)
```python
# Rename existing, correctly typed timestamp columns to standard analysis names.
df_silver = df_bronze.withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
                     .withColumnRenamed("drop_off_datetime", "tpep_dropoff_datetime") 
```

```python
# Apply filters to ensure data reliability and remove invalid records.
df_silver_clean = df_silver.filter(
    # Check for missing/invalid Location IDs (pulocation_id > 0)
    (col("pulocation_id").isNotNull()) & 
    (col("dolocation_id").isNotNull()) & 
    (col("pulocation_id") > 0) & 
    (col("dolocation_id") > 0) & 
    
    # Check for valid Timestamps and temporal consistency (already timestamp type)
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("tpep_dropoff_datetime").isNotNull()) &
    
    # Remove trips where dropoff occurred before pickup (temporal inconsistency)
    (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
```
2. **Gold Layer: Operational Analytics (Advanced SQL)**

   In this step I calculated a very important operational metric: the Week-over-Week growth rate for each dispatching base (Dispatching_base_num). This insight is crucial for a transportation
   company to manage fleet size and resources.
   Key Techniques:
   - **CTES** (WeeklyVolume, RankedPerformance): used to structure complex calculations.
   - **Window functions** (LAG): used to look back at the total_trips from the previous week within the same base partition.
   - **Window Functions** (RANK): used to assign an overall rank based on total volume.

```sql
CREATE OR REPLACE TABLE gold_base_performance AS
WITH WeeklyVolume AS (
    -- 1. Calculate total trip volume per base per calendar week
    SELECT
        dispatching_base_num,
        WEEKOFYEAR(tpep_pickup_datetime) AS pickup_week,
        YEAR(tpep_pickup_datetime) AS pickup_year,
        COUNT(*) AS total_trips
    FROM silver_clean_fhv_data
    GROUP BY 1, 2, 3
),
RankedPerformance AS (
    -- 2. Use a WINDOW FUNCTION (LAG) to fetch the previous week's volume for comparison
    SELECT
        Dispatching_base_num,
        pickup_week,
        total_trips,
        -- Fetch volume from the preceding row within the same Dispatching_base_num partition
        LAG(total_trips, 1, 0) OVER (
            PARTITION BY Dispatching_base_num 
            ORDER BY pickup_year, pickup_week
        ) AS previous_week_trips
    FROM WeeklyVolume
)
-- 3. Select final metrics, calculate growth, and assign overall rank
SELECT
    Dispatching_base_num,
    pickup_week,
    total_trips,
    previous_week_trips,
    -- Calculate Week-over-Week growth percentage
    (total_trips - previous_week_trips) * 100.0 / NULLIF(previous_week_trips, 0) AS week_over_week_growth_percent,
    -- Use another WINDOW FUNCTION (RANK) to classify bases by volume
    RANK() OVER (ORDER BY total_trips DESC) AS overall_volume_rank
FROM RankedPerformance
ORDER BY overall_volume_rank ASC;

-- Display the top-ranked performance metrics
SELECT * FROM gold_base_performance LIMIT 10;
```
**ENVIRONMENTAL SETUP**
- **Platform:** Databricks Serveless Starter Warehouse
- **External Storage:** Google Cloud Storage (GCS)
- **Connector:** Fivetran (GCS to Databricks)
- **Languages:** Pyspark (Python), SQL
- **Data Format:** Delta Lake
