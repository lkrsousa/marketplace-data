Projeto pyspark para leitura de arquivos com dados de pedidos de compra em um marketplace.

- Os arquivos de pedidos chegam via s3 e são lidos em near real time por meio do spark streaming;
- Os dados são lidos conforme parametrização em arquivo yaml;
- Os dados são gravados em tabelas de um banco de dados Postgres.


