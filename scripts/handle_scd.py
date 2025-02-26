from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC_Taxi_SCD_Pipeline") \
    .getOrCreate()

# Define Paths
dim_location_path = "data/processed/dim_location.parquet"
new_location_data_path = "data/raw/location_lookup.parquet"
lookup_table_path = "data/raw/taxi_zone_lookup.csv"

# Load Taxi Zone Lookup Table
taxi_zone_lookup = spark.read.csv(lookup_table_path, header=True, inferSchema=True)

# Rename columns for consistency
taxi_zone_lookup = taxi_zone_lookup.withColumnRenamed("LocationID", "location_id") \
                                   .withColumnRenamed("Borough", "borough") \
                                   .withColumnRenamed("Zone", "zone_name")

# Load New Location Data (NYC Zone Lookup)
new_location_data = taxi_zone_lookup.withColumn("effective_date", current_date()) \
                                    .withColumn("end_date", lit(None).cast("date")) \
                                    .withColumn("is_current", lit(True))

# Try to Load Existing Dimension Table
try:
    dim_location = spark.read.parquet(dim_location_path)
    
    # Mark old records as inactive if they have changed
    changed_records = dim_location.alias("old") \
        .join(new_location_data.alias("new"), "location_id", "inner") \
        .filter(
            (col("old.borough") != col("new.borough")) |
            (col("old.zone_name") != col("new.zone_name"))
        ) \
        .select("old.*") \
        .withColumn("is_current", lit(False)) \
        .withColumn("end_date", current_date())

    # Retain unchanged records
    unchanged_records = dim_location.alias("old") \
        .join(new_location_data.alias("new"), "location_id", "left_anti")

    # Union with new and changed records
    updated_dim_location = unchanged_records.unionByName(changed_records).unionByName(new_location_data)

except Exception:
    # If file does not exist, treat as first load
    updated_dim_location = new_location_data

# Define Window for Deduplication
window_spec = Window.partitionBy("location_id").orderBy(col("effective_date").desc())

# Keep only the latest version of each location_id
updated_dim_location = updated_dim_location.withColumn("row_num", row_number().over(window_spec))
updated_dim_location = updated_dim_location.filter(col("row_num") == 1).drop("row_num")

# Save Updated Dimension Table
updated_dim_location.write.mode("overwrite").parquet(dim_location_path)

print("✅ SCD Type 2 Handling Completed for dim_location Table!")