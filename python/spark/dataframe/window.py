from pyspark.sql.functions import *
from pyspark.sql import SparkSession

delaysPath = 'file:/home/mekwossukoha/sparkData/learningSparkV2/databricks-datasets/learning-spark-v2/flights' \
             '/departuredelays.csv'
airportPath = 'file:/home/mekwossukoha/sparkData/learningSparkV2/databricks-datasets/learning-spark-v2/flights/airport' \
              '-codes-na.txt'

spark = (SparkSession.builder.
         appName("Windowing").
         master("local").
         getOrCreate())
airportsna = (spark.read.format("csv").options(header=True, inferSchema=True, sep="\t")
              .load(airportPath))
airportsna.createOrReplaceTempView("airports_na")

departureDelay = spark.read.format("csv").options(header=True, inferSchema=True).load(delaysPath)
departureDelay.createOrReplaceTempView("departureDelay")
foo = (departureDelay.filter(expr()))

airportsna.show(truncate=False)
departureDelay.show(truncate=False)
