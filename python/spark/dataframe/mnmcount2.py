from __future__ import print_function

import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # print(len(sys.argv))
    # if len(sys.argv) != 2:
    #     print("Usage: mnmcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())
    # get the M&M data set file name
    mnm_file = 'file:/home/mekwossukoha/PycharmProjects/spark/mnm_dataset.csv'
    # read the file into a Spark DataFrame
    mnm_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnm_file)
    mnm_df.show(truncate=False)

    # aggregate count of all colors and groupBy state and color
    # orderBy descending order
    count_mnm_df = (
        mnm_df.select("State", "Color", "Count").groupBy("State", "Color").sum("Count")
        .orderBy("sum(Count)", ascending=False)
        .withColumnRenamed("sum(Count)", 'Total'))

    # total number of rows
    total_row = count_mnm_df.count()

    # show all the resulting aggregation for all the dates and colors
    count_mnm_df.show(n=total_row, truncate=False)
    print("Total Rows = %d" % total_row)

    # find the aggregate count for California by filtering
    ca_count_mnm_df = (count_mnm_df.select("*")
                       .where(count_mnm_df.State == 'CA')
                       .orderBy("Total", ascending=False))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
    spark.stop()
