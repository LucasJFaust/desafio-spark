import unittest
from pyspark.sql import SparkSession
from src import data_processing

class TestDataProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessing") \
            .getOrCreate()

        cls.base_dir = "/home/faust/Projetos/XP/Bootcamp_eng/desafio-spark/dados"
        cls.path_estabelecimentos = [
            f"{cls.base_dir}/estabelecimentos/estabelecimentos-1.csv",
            f"{cls.base_dir}/estabelecimentos/estabelecimentos-2.csv",
            f"{cls.base_dir}/estabelecimentos/estabelecimentos-3.csv"
        ]
        cls.path_cnaes = f"{cls.base_dir}/cnaes/cnaes.csv"

    def test_data_loading(self):
        df_cnaes = data_processing.load_data(self.spark, self.path_cnaes)
        df_estabelecimentos = data_processing.load_data(self.spark, *self.path_estabelecimentos)

        self.assertGreater(df_estabelecimentos.count(), 0, "Estabelecimentos DataFrame should not be empty.")
        self.assertGreater(df_cnaes.count(), 0, "CNAEs DataFrame should not be empty.")

    def test_data_schemas(self):
        df_cnaes = data_processing.load_data(self.spark, self.path_cnaes)
        df_estabelecimentos = data_processing.load_data(self.spark, *self.path_estabelecimentos)

        expected_estabelecimentos_columns = 30
        expected_cnaes_columns = 2

        self.assertEqual(len(df_estabelecimentos.columns), expected_estabelecimentos_columns, "Estabelecimentos DataFrame should have 30 columns.")
        self.assertEqual(len(df_cnaes.columns), expected_cnaes_columns, "CNAEs DataFrame should have 2 columns.")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
