from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, unix_timestamp, hour, dayofweek, when, 
    year, month, dayofmonth, round, sum
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC_Taxi_ETL_Pipeline") \
    .getOrCreate()

# Load Taxi Zone Lookup Table
taxi_zone_lookup = spark.read.csv("data/raw/taxi_zone_lookup.csv", header=True, inferSchema=True)

# Rename columns for consistency
taxi_zone_lookup = taxi_zone_lookup.withColumnRenamed("LocationID", "location_id") \
                                   .withColumnRenamed("Borough", "borough") \
                                   .withColumnRenamed("Zone", "zone_name")

# Load All Green & Yellow Taxi Data (Multiple Files)
green_taxi_data = spark.read.parquet("data/raw/green_tripdata_*.parquet")
yellow_taxi_data = spark.read.parquet("data/raw/yellow_tripdata_*.parquet")

# Standardize Column Names
green_taxi_data = green_taxi_data.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime") \
                                 .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")

# Add missing `trip_type` column to Yellow Taxi Data
yellow_taxi_data = yellow_taxi_data.withColumn("trip_type", lit(None).cast("integer"))

# Add missing `airport_fee` column to Green Taxi Data
green_taxi_data = green_taxi_data.withColumn("airport_fee", lit(None).cast("double"))

# Add missing `ehail_fee` column to Yellow Taxi Data (since Green Taxi has it)
yellow_taxi_data = yellow_taxi_data.withColumn("ehail_fee", lit(None).cast("double"))

# Reorder Columns to Ensure Both DataFrames Have the Exact Same Order
column_order = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag",
    "RatecodeID", "PULocationID", "DOLocationID", "passenger_count", "trip_distance",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "ehail_fee",
    "improvement_surcharge", "total_amount", "payment_type", "trip_type", "congestion_surcharge", "airport_fee"
]

yellow_taxi_data = yellow_taxi_data.select(*column_order)
green_taxi_data = green_taxi_data.select(*column_order)

# Now, both datasets have the same schema and can be merged
taxi_df = yellow_taxi_data.unionByName(green_taxi_data)

# Data Cleaning
taxi_df = taxi_df.filter((col("fare_amount") > 0) & (col("trip_distance") > 0))
taxi_df = taxi_df.dropDuplicates()

# **Feature Engineering**
## Calculate trip duration (in minutes)
taxi_df = taxi_df.withColumn("trip_duration", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60)

## Calculate trip speed (miles per hour)
taxi_df = taxi_df.withColumn("trip_speed", 
    round(col("trip_distance") / (col("trip_duration") / 60), 2))

## Identify outliers in fare amounts
taxi_df = taxi_df.withColumn("is_fare_outlier", 
    when(col("fare_amount") > 200, True).otherwise(False))

## Categorize fares
taxi_df = taxi_df.withColumn("fare_category", 
    when(col("fare_amount") < 10, "Low Fare")
    .when(col("fare_amount") < 30, "Medium Fare")
    .otherwise("High Fare"))

# Join with Taxi Zone Lookup for Pickup Locations
taxi_df = taxi_df.join(
    taxi_zone_lookup, 
    taxi_df.PULocationID == taxi_zone_lookup.location_id, 
    "left"
).withColumnRenamed("borough", "pickup_borough") \
 .withColumnRenamed("zone_name", "pickup_zone") \
 .withColumnRenamed("service_zone", "pickup_service_zone") \
 .drop("location_id")

# Join with Taxi Zone Lookup for Dropoff Locations
taxi_df = taxi_df.join(
    taxi_zone_lookup, 
    taxi_df.DOLocationID == taxi_zone_lookup.location_id, 
    "left"
).withColumnRenamed("borough", "dropoff_borough") \
 .withColumnRenamed("zone_name", "dropoff_zone") \
 .withColumnRenamed("service_zone", "dropoff_service_zone") \
 .drop("location_id")

# Extract year, month, and day from pickup timestamp
taxi_df = taxi_df.withColumn("year", year(col("tpep_pickup_datetime"))) \
                 .withColumn("month", month(col("tpep_pickup_datetime"))) \
                 .withColumn("day", dayofmonth(col("tpep_pickup_datetime")))

# Aggregation: Daily and hourly revenue
daily_revenue = taxi_df.groupBy("VendorID", "tpep_pickup_datetime").agg(sum("total_amount").alias("daily_revenue"))
hourly_revenue = taxi_df.groupBy("VendorID", "tpep_pickup_datetime").agg(sum("total_amount").alias("hourly_revenue"))

# Filter for 2024 trips
year_2024_trips = taxi_df.filter(col("tpep_pickup_datetime").like("2024%"))

# Partition & Save Processed Data
output_path = "data/processed/fact_trips/"
taxi_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)

print("✅ NYC Taxi ETL Pipeline Completed Successfully!")