from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("DellAcademy").getOrCreate()

df = spark.read.parquet("gs://myprojecttest-429801/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

gender_distribution = df.groupBy("gender").agg(count("gender").alias("total_count"))

gender_distribution.show()

spark.stop()