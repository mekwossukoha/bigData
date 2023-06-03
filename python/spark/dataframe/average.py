from __future__ import print_function
from pyspark.sql import functions
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import avg, col, column, expr, count
from pyspark.sql import functions

if __name__ == '__main__':
    print(len(sys.argv))
    # if len(sys.argv) != 2:
    # print('Usage: average.py <file>', file=sys.stderr)
    # sys.exit(45)

    # spark entry point
    spark = (SparkSession.
             builder.
             master('local').
             appName("average").
             getOrCreate())
    # input file to read
    # my_file = sys.argv[1]
    my_file = 'file:/home/mekwossukoha/PycharmProjects/spark/mnm_dataset.csv'
    df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(my_file)
    df1 = df.select("State", "Color", "Count").groupby("State", "Color").agg(count("Count"))
    df1.show(truncate=False)
    number_of_rows = df.count()
    print(f"Total number of rows is {number_of_rows}")
