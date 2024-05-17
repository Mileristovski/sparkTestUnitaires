from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo1.main import wordcount
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_wordcount(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(text="bonjour je suis un test unitaire"),
                Row(text="bonjour suis test"),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(cleanword="bonjour", count=2),
                Row(cleanword="je", count=1),
                Row(cleanword="suis", count=2),
                Row(cleanword="un", count=1),
                Row(cleanword="test", count=2),
                Row(cleanword="unitaire", count=1),
            ]
        )

        actual = wordcount(input, "text")

        self.assertCountEqual(actual.collect(), expected.collect())
        self.assertCountEqual(actual.columns, expected.columns)
