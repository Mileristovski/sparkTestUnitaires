from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import (
    get_adult_and_city,
    join_by_zip,
    filter_by_age,
    add_departement,
)
from pyspark.sql import Row
from pyspark.errors.exceptions.captured import AnalysisException


class TestSparkCleanJob(unittest.TestCase):
    def test_func_sucess_filter_by_age(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Toto", age=23, zip=28700),
                Row(name="Tata", age=15, zip=28700),
                Row(name="Titi", age=32, zip=28700),
            ]
        )
        expected = spark.createDataFrame(
            [Row(name="Toto", age=23, zip=28700), Row(name="Titi", age=32, zip=28700)]
        )

        actual = filter_by_age(input)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)

    def test_func_error_filter_by_age(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Toto", zip=28700),
                Row(name="Tata", zip=28700),
                Row(name="Titi", zip=28700),
            ]
        )

        with self.assertRaises(AttributeError):
            filter_by_age(input)
            

    def test_func_sucess_join_by_zip(self):
        # GIVEN
        input_client = spark.createDataFrame(
            [
                Row(name="Toto", age=23, zip_clients=25650),
                Row(name="Tata", age=15, zip_clients=25640),
                Row(name="Titi", age=32, zip_clients=25650),
            ]
        )
        input_city = spark.createDataFrame(
            [
                Row(zip=25650, city="VILLE DU PONT"),
                Row(zip=25640, city="VILLERS GRELOT"),
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(name="Toto", age=23, zip=25650, city="VILLE DU PONT"),
                Row(name="Tata", age=15, zip=25640, city="VILLERS GRELOT"),
                Row(name="Titi", age=32, zip=25650, city="VILLE DU PONT"),
            ]
        )

        actual = join_by_zip(input_client, "zip_clients", input_city, "zip")

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)

    def test_func_sucess_add_departement(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Toto", age=23, zip=25650, city="VILLE DU PONT"),
                Row(name="Tata", age=67, zip=20167, city="APPIETTO"),
                Row(name="Titi", age=32, zip=70360, city="70360"),
                Row(name="Tutu", age=45, zip=20251, city="ALTIANI"),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(
                    name="Toto", age=23, zip=25650, city="VILLE DU PONT", departement=25
                ),
                Row(name="Tata", age=67, zip=20167, city="APPIETTO", departement="2A"),
                Row(name="Titi", age=32, zip=70360, city="70360", departement=70),
                Row(name="Tutu", age=45, zip=20251, city="ALTIANI", departement="2B"),
            ]
        )

        actual = add_departement(input)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)

    def test_func_error_add_departement(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Toto", age=23, city="VILLE DU PONT"),
                Row(name="Tata", age=67, city="APPIETTO"),
                Row(name="Titi", age=32, city="70360"),
                Row(name="Tutu", age=45, city="ALTIANI"),
            ]
        )

        with self.assertRaises(AnalysisException):
            add_departement(input)

    def test_inte_get_adult_and_city(self):
        # GIVEN
        input_clients = spark.createDataFrame(
            [
                Row(name="Toto", age=23, zip_clients=25650),
                Row(name="Tata", age=67, zip_clients=20167),
                Row(name="Titi", age=32, zip_clients=70360),
                Row(name="Tutu", age=45, zip_clients=20251),
                Row(name="Pierre", age=17, zip_clients=30260),
            ]
        )
        input_city = spark.createDataFrame(
            [
                Row(zip=25650, city="VILLE DU PONT"),
                Row(zip=20167, city="APPIETTO"),
                Row(zip=70360, city="70360"),
                Row(zip=20251, city="ALTIANI"),
                Row(zip=30260, city="VIC LE FESQ"),
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(
                    name="Toto", age=23, zip=25650, city="VILLE DU PONT", departement=25
                ),
                Row(name="Tata", age=67, zip=20167, city="APPIETTO", departement="2A"),
                Row(name="Titi", age=32, zip=70360, city="70360", departement=70),
                Row(name="Tutu", age=45, zip=20251, city="ALTIANI", departement="2B"),
            ]
        )

        actual = get_adult_and_city(input_clients, input_city)

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)
