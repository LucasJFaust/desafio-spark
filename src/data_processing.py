from pyspark.sql import SparkSession
import src.spark_setup as spark_setup

def load_data(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True, sep=';')

def main():
    spark = spark_setup.get_spark_session()

    # Carregar dados
    cnaes_path = "dados/cnaes/cnaes.csv"
    estabelecimentos_path1 = "dados/estabelecimentos/estabelecimentos-1.csv"
    estabelecimentos_path2 = "dados/estabelecimentos/estabelecimentos-2.csv"
    estabelecimentos_path3 = "dados/estabelecimentos/estabelecimentos-3.csv"

    df_cnaes = load_data(spark, cnaes_path)
    df_estabelecimentos1 = load_data(spark, estabelecimentos_path1)
    df_estabelecimentos2 = load_data(spark, estabelecimentos_path2)
    df_estabelecimentos3 = load_data(spark, estabelecimentos_path3)

    # Unir os DataFrames de estabelecimentos
    df_estabelecimentos = df_estabelecimentos1.union(df_estabelecimentos2).union(df_estabelecimentos3)

    # Mostrar uma amostra dos dados
    df_cnaes.show(5)
    df_estabelecimentos.show(5)

if __name__ == "__main__":
    main()
