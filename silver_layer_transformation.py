from pyspark.sql.functions import col

# --- CONFIGURATION: UNITY CATALOG PATH ---
# Using the correct path identified in the Serverless Starter Warehouse environment.
TABLE_NAME_BRONZE = "workspace.fhv_data_pipeline.raw_fhv_trips" 

print(f"Starting Silver Layer transformation. Reading from: {TABLE_NAME_BRONZE}...")

# 1. READ DATA from the Bronze Layer
df_bronze = spark.read.table(TABLE_NAME_BRONZE)

# 2. SCHEMA TRANSFORMATION 
# Rename existing, correctly typed timestamp columns to standard analysis names.
df_silver = df_bronze.withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
                     .withColumnRenamed("drop_off_datetime", "tpep_dropoff_datetime") 

# 3. DATA QUALITY FILTERING (Using corrected lowercase column names)
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
    
# Drop Fivetran/system columns not needed for analysis
).drop("_file", "_line", "_modified", "_fivetran_synced", "sr_flag", "affiliated_base_number")

# 4. WRITE DATA to the Silver Layer
# Save the cleaned, transformed data as a new Delta table.
df_silver_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_clean_fhv_data")

# Display the final count of cleaned records.
print(f"Successfully created Silver Layer table: 'silver_clean_fhv_data'.")
print(f"Cleaned Record Count: {df_silver_clean.count()}")
