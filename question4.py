from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg, count

spark = SparkSession.builder.appName("DellAcademy").getOrCreate()

df = spark.read.parquet("gs://myprojecttest-429801/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

bike_trip_count = df.groupBy("bikeid").agg(count("bikeid").alias("total_trips"))

most_used_bikes = bike_trip_count.orderBy(col("total_trips").desc()).limit(10)

most_used_bikes.show()

spark.stop()