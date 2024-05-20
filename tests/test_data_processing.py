import pytest
from pyspark.sql import SparkSession
from src.data_processing import load_data, get_total_estabelecimentos, get_column_info

# Configurar a sessão Spark para testes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("Testes Desafio Modulo 3") \
        .master("local[2]") \
        .getOrCreate()

# Teste para verificar o carregamento dos dados
def test_load_data(spark):
    path = "dados/cnaes/cnaes.csv"
    df = load_data(spark, path)
    assert df is not None
    assert df.count() > 0

# Teste para verificar a contagem de estabelecimentos
def test_get_total_estabelecimentos(spark):
    path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df = load_data(spark, path)
    total = get_total_estabelecimentos(df)
    assert total > 0

# Teste para verificar as informações de colunas
def test_get_column_info(spark):
    path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df = load_data(spark, path)
    num_columns, num_numeric_columns = get_column_info(df)
    assert num_columns == 30
    assert num_numeric_columns > 0

# Continuar os imports e configuração do Spark

# Teste para verificar contagem de logradouros faltantes
def test_count_missing_logradouro(spark):
    path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df = load_data(spark, path)
    missing_logradouro_count = count_missing_logradouro(df, spark)
    assert missing_logradouro_count >= 0

# Teste para verificar contagem de estabelecimentos em avenida
def test_count_avenida(spark):
    path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df = load_data(spark, path)
    avenida_count = count_avenida(df)
    assert avenida_count >= 0

# Teste para verificar contagem de CEPs distintos
def test_count_unique_ceps(spark):
    path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df = load_data(spark, path)
    unique_ceps = count_unique_ceps(df)
    assert unique_ceps > 0

# Teste para verificar contagem de CNAEs
def test_count_cnaes(spark):
    path = "dados/cnaes/cnaes.csv"
    df = load_data(spark, path)
    total_cnaes = count_cnaes(df)
    assert total_cnaes > 0

# Teste para verificar contagem de estabelecimentos relacionados a cultivo
def test_count_cultivo_estabelecimentos(spark):
    cnaes_path = "dados/cnaes/cnaes.csv"
    estabelecimentos_path = "dados/estabelecimentos/estabelecimentos-1.csv"
    df_cnaes = load_data(spark, cnaes_path)
    df_estabelecimentos = load_data(spark, estabelecimentos_path)
    cultivo_count = count_cultivo_estabelecimentos(df_estabelecimentos, df_cnaes)
    assert cultivo_count >= 0
