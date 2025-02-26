from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, unix_timestamp, hour, dayofweek, when, year, month, dayofmonth

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
combined_data = yellow_taxi_data.unionByName(green_taxi_data)

# Data Cleaning
combined_data = combined_data.filter((col("fare_amount") > 0) & (col("trip_distance") > 0))
combined_data = combined_data.dropDuplicates()

# Feature Engineering
combined_data = combined_data.withColumn("trip_duration", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60)
combined_data = combined_data.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
combined_data = combined_data.withColumn("pickup_day", dayofweek(col("tpep_pickup_datetime")))
combined_data = combined_data.withColumn("fare_category", 
    when(col("fare_amount") < 10, "Low Fare")
    .when(col("fare_amount") < 30, "Medium Fare")
    .otherwise("High Fare"))

# Join with Taxi Zone Lookup for Pickup Locations
combined_data = combined_data.join(
    taxi_zone_lookup, 
    combined_data.PULocationID == taxi_zone_lookup.location_id, 
    "left"
).withColumnRenamed("borough", "pickup_borough") \
 .withColumnRenamed("zone_name", "pickup_zone") \
 .withColumnRenamed("service_zone", "pickup_service_zone") \
 .drop("location_id")

# Join with Taxi Zone Lookup for Dropoff Locations
combined_data = combined_data.join(
    taxi_zone_lookup, 
    combined_data.DOLocationID == taxi_zone_lookup.location_id, 
    "left"
).withColumnRenamed("borough", "dropoff_borough") \
 .withColumnRenamed("zone_name", "dropoff_zone") \
 .withColumnRenamed("service_zone", "dropoff_service_zone") \
 .drop("location_id")

# Extract year, month, and day from pickup timestamp
combined_data = combined_data.withColumn("year", year(col("tpep_pickup_datetime"))) \
                             .withColumn("month", month(col("tpep_pickup_datetime"))) \
                             .withColumn("day", dayofmonth(col("tpep_pickup_datetime")))

# Partition & Save Processed Data
combined_data.write.mode("overwrite").partitionBy("year", "month", "day").parquet("data/processed/fact_trips/")

print("✅ NYC Taxi ETL Pipeline Completed Successfully!")