from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

def load_data(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True, sep=';')

def get_total_estabelecimentos(df):
    return df.count()

def get_column_info(df):
    num_columns = len(df.columns)
    num_numeric_columns = len([field for field in df.schema.fields if str(field.dataType) in ['IntegerType', 'DoubleType']])
    return num_columns, num_numeric_columns

def get_space_saving(original_paths, parquet_path):
    import os

    original_size = sum(os.path.getsize(path) for path in original_paths)
    parquet_size = sum(os.path.getsize(os.path.join(parquet_path, file)) for file in os.listdir(parquet_path))

    return original_size, parquet_size

def count_missing_logradouro(df, spark):
    df.createOrReplaceTempView("estabelecimentos")
    result = spark.sql("SELECT COUNT(*) FROM estabelecimentos WHERE LOGRADOURO IS NULL")
    return result.collect()[0][0]

def count_avenida(df):
    def is_avenida(logradouro):
        return logradouro.startswith("AVENIDA")

    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType

    is_avenida_udf = udf(is_avenida, BooleanType())
    df = df.withColumn("is_avenida", is_avenida_udf(col("LOGRADOURO")))

    df.createOrReplaceTempView("estabelecimentos")
    result = spark.sql("SELECT COUNT(*) FROM estabelecimentos WHERE is_avenida = True")
    return result.collect()[0][0]

def count_unique_ceps(df):
    return df.select(countDistinct("CEP")).collect()[0][0]

def count_cnaes(df):
    return df.count()

def count_cultivo_estabelecimentos(df_estabelecimentos, df_cnaes):
    def is_cultivo(descricao):
        return "cultivo" in descricao.lower()

    from pyspark.sql.functions import udf
    from pyspark.sql.types import BooleanType

    is_cultivo_udf = udf(is_cultivo, BooleanType())
    df_cnaes = df_cnaes.withColumn("is_cultivo", is_cultivo_udf(col("DESCRICAO")))

    df = df_estabelecimentos.join(df_cnaes, df_estabelecimentos.CNAE_FISCAL_PRINCIPAL == df_cnaes.CODIGO)
    df = df.filter(df.is_cultivo)

    return df.count()

def main():
    spark = SparkSession.builder.appName("Desafio Modulo 3").getOrCreate()

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

    # Pergunta 1: Quantos estabelecimentos existem?
    total_estabelecimentos = get_total_estabelecimentos(df_estabelecimentos)
    print(f"Total de estabelecimentos: {total_estabelecimentos}")

    # Pergunta 2: Quantas colunas e quantas são identificadas como números?
    num_columns, num_numeric_columns = get_column_info(df_estabelecimentos)
    print(f"Total de colunas: {num_columns}, Colunas numéricas: {num_numeric_columns}")

    # Pergunta 3: Economia de espaço com Parquet
    original_paths = [estabelecimentos_path1, estabelecimentos_path2, estabelecimentos_path3]
    parquet_path = "dados/estabelecimentos_parquet"
    df_estabelecimentos.write.parquet(parquet_path)
    original_size, parquet_size = get_space_saving(original_paths, parquet_path)
    print(f"Original size: {original_size} bytes, Parquet size: {parquet_size} bytes")

    # Pergunta 4: Quantos estabelecimentos não têm logradouro cadastrado?
    missing_logradouro_count = count_missing_logradouro(df_estabelecimentos, spark)
    print(f"Estabelecimentos sem logradouro: {missing_logradouro_count}")

    # Pergunta 5: Quantos estabelecimentos têm logradouro como 'AVENIDA'?
    avenida_count = count_avenida(df_estabelecimentos)
    print(f"Estabelecimentos com logradouro 'AVENIDA': {avenida_count}")

    # Pergunta 6: Quantos CEPs distintos existem?
    distinct_ceps = count_unique_ceps(df_estabelecimentos)
    print(f"Total de CEPs distintos: {distinct_ceps}")

    # Pergunta 7: Quantos CNAEs existem na tabela de CNAEs?
    total_cnaes = count_cnaes(df_cnaes)
    print(f"Total de CNAEs: {total_cnaes}")

    # Pergunta 8: Quantos estabelecimentos possuem um CNAE relacionado a cultivo?
    cultivo_estabelecimentos_count = count_cultivo_estabelecimentos(df_estabelecimentos, df_cnaes)
    print(f"Estabelecimentos relacionados a cultivo: {cultivo_estabelecimentos_count}")

if __name__ == "__main__":
    main()
