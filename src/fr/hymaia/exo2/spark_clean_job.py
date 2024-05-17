import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("clean_job").master("local[*]").getOrCreate()

    df_city = spark.read.option("header", True).csv(
        "src/resources/exo2/city_zipcode.csv"
    )
    df_clients = (
        spark.read.option("header", True)
        .csv("src/resources/exo2/clients_bdd.csv")
        .withColumnRenamed("zip", "zip_clients")
    )

    res = get_adult_and_city(df_clients, df_city)
    res.write.mode("overwrite").parquet("data/exo2/clean")


def get_adult_and_city(df_clients, df_city):
    df_clients_majeurs = filter_by_age(df_clients)
    df_clients_majeurs_joined = join_by_zip(
        df_clients_majeurs, "zip_clients", df_city, "zip"
    )
    res = add_departement(df_clients_majeurs_joined)

    return res


def filter_by_age(df):
    return df.filter(df.age > 18)


def join_by_zip(df_client, df_client_column, df_city, df_city_column):
    return df_client.join(
        df_city, df_client[df_client_column] == df_city[df_city_column], "left"
    ).drop(df_client_column)


def add_departement(df):
    return df.withColumn(
        "departement",
        f.when((f.col("zip") <= 20190) & (f.col("zip").startswith("20")), "2A")
        .when((f.col("zip") > 20190) & (f.col("zip").startswith("20")), "2B")
        .otherwise(f.substring("zip", 1, 2)),
    )
