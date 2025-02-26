from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, avg, sum, count, when, round, unix_timestamp, sha2, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYC_Taxi_Driver_Performance") \
    .getOrCreate()

# Define Paths
dim_location_path = "data/processed/dim_location.parquet"
fact_trips_path = "data/processed/fact_trips/"
driver_performance_path = "data/processed/driver_performance.parquet"
lookup_table_path = "data/raw/taxi_zone_lookup.csv"

# Load Taxi Zone Lookup Table
taxi_zone_lookup = spark.read.csv(lookup_table_path, header=True, inferSchema=True)

# Rename columns for consistency
taxi_zone_lookup = taxi_zone_lookup.withColumnRenamed("LocationID", "location_id") \
                                   .withColumnRenamed("Borough", "borough") \
                                   .withColumnRenamed("Zone", "zone_name")

# Load Processed Taxi Trip Data
fact_trips = spark.read.parquet(fact_trips_path)

# Generate a pseudo driver_id based on VendorID, PULocationID, and DOLocationID
fact_trips = fact_trips.withColumn("driver_id", sha2(concat_ws("_", col("VendorID"), col("PULocationID"), col("DOLocationID")), 256))

# Ensure all necessary columns exist
fact_trips = fact_trips.withColumn("trip_duration", 
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
)

fact_trips = fact_trips.withColumn("trip_speed", 
    round(col("trip_distance") / (col("trip_duration") / 60), 2))

fact_trips = fact_trips.withColumn("tip_percentage",
    when(col("fare_amount") > 0, round((col("tip_amount") / col("fare_amount")) * 100, 2))
    .otherwise(lit(0))
)

# Aggregate Metrics per Driver
driver_performance = fact_trips.groupBy("driver_id").agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("trip_duration").alias("avg_trip_duration"),
    avg("fare_amount").alias("avg_fare_per_trip"),
    sum("trip_distance").alias("total_miles"),
    avg("trip_speed").alias("avg_speed"),
    avg("tip_percentage").alias("avg_tip_percentage")
)

# Load Existing Driver Performance Table
try:
    existing_driver_perf = spark.read.parquet(driver_performance_path)

    # Merge Old and New Records
    window_spec = Window.partitionBy("driver_id").orderBy(col("total_trips").desc())
    updated_driver_performance = existing_driver_perf.unionByName(driver_performance) \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1).drop("row_num")

except Exception:
    # First time writing the file
    updated_driver_performance = driver_performance

# Save Updated Driver Performance Metrics
updated_driver_performance.write.mode("overwrite").parquet(driver_performance_path)

print("✅ Driver Performance Metrics Updated Successfully!")