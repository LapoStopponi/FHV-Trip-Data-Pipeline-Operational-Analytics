
-- GOLD LAYER: ADVANCED OPERATIONAL ANALYTICS
-- This query identifies base performance metrics (Dispatching_base_num)
-- It uses Window Functions (LAG and RANK) to calculate Week-over-Week growth.



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
