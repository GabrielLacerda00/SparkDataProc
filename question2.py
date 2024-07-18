from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg

spark = SparkSession.builder.appName("DellAcademy").getOrCreate()

df = spark.read.parquet("gs://myprojecttest-429801/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

df = df.withColumn("duration", (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60)

df = df.filter(df.duration > 0)

df_avg_duration = df.agg(avg("duration").alias("average_duration"))

df_avg_duration.show()

df_fastest_trips = df.orderBy(col("duration").asc()).limit(10)
df_fastest_trips.show()

spark.stop()