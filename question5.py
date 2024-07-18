from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg, count

spark = SparkSession.builder.appName("DellAcademy").getOrCreate()

df = spark.read.parquet("gs://myprojecttest-429801/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

df = df.withColumn("age", 2024 - df["birth_year"])

df = df.filter(df.age.isNotNull() & (df.age > 0))

avg_age = df.agg(avg("age").alias("average_age"))

avg_age.show()

spark.stop()