import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("wordcount").master("local[*]").getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")

    res = wordcount(df, "text")

    res.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")


def wordcount(df, col_name):
    return (
        df.withColumn("word", f.explode(f.split(f.col(col_name), " ")))
        .withColumn("cleanword", f.regexp_replace(f.lower(f.col("word")), "[,;.]", ""))
        .groupBy("cleanword")
        .count()
    )
