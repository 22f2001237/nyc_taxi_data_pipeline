from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc
from pyspark.sql.functions import dayofweek

# Initialize Spark Session
spark = SparkSession.builder.appName("NYC_Taxi_Analytics").getOrCreate()

# Load Processed Data
fact_trips = spark.read.parquet("data/processed/fact_trips/")

# Top 10 Pickup Locations by Revenue
top_pickup_revenue = fact_trips.groupBy("PULocationID").agg(
    sum("total_amount").alias("total_revenue")
).orderBy(desc("total_revenue")).limit(10)

top_pickup_revenue.show()

# Trip patterns during weekdays vs weekends
fact_trips = fact_trips.withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
weekday_vs_weekend = fact_trips.groupBy("day_of_week").count()
weekday_vs_weekend.show()

# Top 10 Long Trip Analysis (trips > 10 miles)
long_trips = fact_trips.filter(col("trip_distance") > 10)
top_long_trips = long_trips.orderBy(desc("trip_distance")).limit(10)
top_long_trips.show()