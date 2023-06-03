from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .appName("Family")
         .getOrCreate())
# creating a schema
schema = "Name STRING, Age INT, Gender STRING"

rows = [Row("Ukoha Emekwo", 29, 'M'), Row("Jane Kalu", 34, 'F'), Row("Ugo Onyeani", 37, 'F')]
df = spark.createDataFrame(rows, schema)
df.show()
df1 = df.where('Age % 2 = 1').show()
df1 = df.where('Name == "Ukoha Emekwo"').show()