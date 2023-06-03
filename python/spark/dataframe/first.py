from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# creating an entry point to spark
spark = SparkSession.builder.appName("first").master('local').getOrCreate()
file = 'file:/usr/lib/spark/README.md'
df = spark.read.text(file)
df.show(truncate=False)
df.filter(col('value').startswith('Spark')).show(truncate=False)