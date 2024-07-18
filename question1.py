from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

spark = SparkSession.builder.appName("DellAcademy").getOrCreate()

df = spark.read.parquet("gs://myprojecttest-429801/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

trip_avg_duration = df.groupBy("start_station_id", "end_station_id").agg(avg("tripduration").alias("avg_tripduration"))

slowest_trips_avd_duration = trip_avg_duration.orderBy(col("avg_tripduration").desc()).limit(10)

slowest_trips_avd_duration.show()