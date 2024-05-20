import sys
import os

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import spark_setup

def load_data(spark, *paths):
    if len(paths) == 1:
        return spark.read.csv(paths[0], header=True, inferSchema=True, sep=';')
    else:
        dfs = [spark.read.csv(path, header=True, inferSchema=True, sep=';') for path in paths]
        df_final = dfs[0]
        for df in dfs[1:]:
            df_final = df_final.union(df)
        return df_final

def main():
    spark = spark_setup.get_spark_session()

    # Carregar dados
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cnaes_path = os.path.join(base_dir, "../dados/cnaes/cnaes.csv")
    estabelecimentos_paths = [
        os.path.join(base_dir, "../dados/estabelecimentos/estabelecimentos-1.csv"),
        os.path.join(base_dir, "../dados/estabelecimentos/estabelecimentos-2.csv"),
        os.path.join(base_dir, "../dados/estabelecimentos/estabelecimentos-3.csv")
    ]

    df_cnaes = load_data(spark, cnaes_path)
    df_estabelecimentos = load_data(spark, *estabelecimentos_paths)

    # Mostrar uma amostra dos dados
    df_cnaes.show(5)
    df_estabelecimentos.show(5)

if __name__ == "__main__":
    main()

