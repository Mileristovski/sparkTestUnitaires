from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_aggregate_job import get_population_by_departement, aggregate_population_by_departement
from pyspark.sql import Row


class TestSparkAggregateJob(unittest.TestCase):
    def test_func_get_population_by_departement(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Tete", age=32, zip=74330, city="NONGLARD", departement=74),
                Row(name="Tete2", age=39, zip=74330, city="NONGLARD", departement=74),
                Row(name="Toto", age=23, zip=27120, city="BONCOURT", departement=25),
                Row(name="Toto2", age=89, zip=27120, city="BONCOURT", departement=25),
                Row(name="Tata", age=67, zip=20167, city="APPIETTO", departement="2A"),
                Row(name="Titi", age=32, zip=70360, city="70360", departement=70),
                Row(name="Titi2", age=30, zip=70360, city="70360", departement=70),
                Row(name="Titi3", age=39, zip=70360, city="70360", departement=70),
                Row(name="Titi4", age=32, zip=70360, city="70360", departement=70),
                Row(name="Tutu", age=45, zip=20251, city="ALTIANI", departement="2B"),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(departement=70, nb_people=4),
                Row(departement=25, nb_people=2),
                Row(departement=74, nb_people=2),
                Row(departement="2A", nb_people=1),
                Row(departement="2B", nb_people=1),
            ]
        )

        actual = get_population_by_departement(input)

        self.assertEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.columns, expected.columns)
    

    def test_inte_aggregate_population_by_departement(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name="Tete", age=32, zip=74330, city="NONGLARD", departement=74),
                Row(name="Tete2", age=39, zip=74330, city="NONGLARD", departement=74),
                Row(name="Toto", age=23, zip=27120, city="BONCOURT", departement=25),
                Row(name="Toto2", age=89, zip=27120, city="BONCOURT", departement=25),
                Row(name="Tata", age=67, zip=20167, city="APPIETTO", departement="2A"),
                Row(name="Titi", age=32, zip=70360, city="70360", departement=70),
                Row(name="Titi2", age=30, zip=70360, city="70360", departement=70),
                Row(name="Titi3", age=39, zip=70360, city="70360", departement=70),
                Row(name="Titi4", age=32, zip=70360, city="70360", departement=70),
                Row(name="Tutu", age=45, zip=20251, city="ALTIANI", departement="2B"),
            ]
        )

        expected = spark.createDataFrame(
            [
                Row(departement=70, nb_people=4),
                Row(departement=25, nb_people=2),
                Row(departement=74, nb_people=2),
                Row(departement="2A", nb_people=1),
                Row(departement="2B", nb_people=1),
            ]
        )

        actual = aggregate_population_by_departement(input)

        self.assertEqual(actual.collect(), expected.collect())
        self.assertEqual(actual.columns, expected.columns)
