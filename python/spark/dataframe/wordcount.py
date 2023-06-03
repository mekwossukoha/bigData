from pyspark.sql import SparkSession
import sys

spark = (SparkSession.
         builder.
         appName("wordCount").
         master("local").
         getOrCreate())
if __name__ == '__main__':
    # if len(sys.argv) != 2:
    #     print('Usage: <file> <input> <fileFormat>')
    #     sys.exit(21)
    file_name = 'file:/home/mekwossukoha/sparkData/wordcount.txt'
    df = spark.sparkContext.textFile(file_name)
    df1 = df.flatMap(lambda x: x.split(" ")).map(lambda x: x.lower()).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).toDF(["word","count"])
    df1.write.format("csv").saveAsTable('file:/home/mekwossukoha/sparkData/wordcount1')