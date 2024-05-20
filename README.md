# desafio-spark

# Enunciado
Bem-vindos ao Desafio do Módulo 3!

Nele, você vai utilizar o Spark SQL e UDFs para responder a algumas perguntas a partir de dados de cadastro de estabelecimentos brasileiros.
Você vai trabalhar com os seguintes arquivos, disponíveis em:

http://www.dcc.ufmg.br/~pcalais/XPE/engenharia-dados/big-data-spark/desafio.

São dois conjuntos de arquivos:
• CNAEs: contém o código do CNAE (Classificação Nacional de Atividades Econômicas) e a descrição textual de cada um. Por exemplo, o CNAE 0116403 indica que a atividade de um estabelecimento com este CNAE é “Cultivo de Mamona”.
• Estabelecimentos: contém o registro de estabelecimentos e diversos metadados, como CNPJ, o código do CNAE, endereço, telefone, entre outros.

Consulte o arquivo NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf para checar o esquema de dados e explicação mais detalhada de cada campo.
Utilize o Apache Spark para ler os dados e responder às questões propostas. Divirta-se!

# Estrutura do Projeto

desafio_modulo3/
│
├── dados/
│   ├── cnaes/
│   │   ├── cnaes.csv
│   ├── estabelecimentos/
│   │   ├── estabelecimentos-1.csv
│   │   ├── estabelecimentos-2.csv
│   │   ├── estabelecimentos-3.csv
│
├── src/
│   ├── __init__.py
│   ├── data_processing.py
│   ├── spark_setup.py
│   └── queries.py
│
├── tests/
│   ├── __init__.py
│   └── test_data_processing.py
│
├── .gitignore
├── pyproject.toml
├── README.md


Qualquer dúvida fico à disposição.
https://www.linkedin.com/in/lucas-jose-faust-machado/
https://github.com/LucasJFaust
