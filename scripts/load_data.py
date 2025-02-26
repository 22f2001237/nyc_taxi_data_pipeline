from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("nyc_taxi_data_pipeline").getOrCreate()

filepath = "data/raw/green_tripdata_2024-09.parquet"

df = spark.read.parquet(filepath)
df.show(5)

spark.stop()
