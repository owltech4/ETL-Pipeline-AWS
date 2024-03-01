import sys
import pandas as pd
import awswrangler as wr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicializa os argumentos esperados pelo Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Inicializa o contexto do Spark e do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define o caminho do seu bucket de entrada
input_s3_path = "s3://bucket-study-marcelo/State of Data 2021 - Dataset - Pgina1.csv"

# Lê o arquivo do S3 usando Spark
df_spark = spark.read.csv(input_s3_path, header=True)

# Mostra os resultados (Isso será visível nos logs do Glue)
df_spark.show()

# Converte o DataFrame do Spark para um DataFrame do Pandas
# Atenção: Esta operação pode ser custosa em termos de memória para DataFrames grandes
df_pandas = df_spark.toPandas()

# Define a configuração de saída para o S3 e o Athena
output_s3_path = "s3://my-bucket-athena-digo/pasta-de-destino/"
database_name = "workspace_db"
table_name = "tb_datahackers_2021"

# Escreve o DataFrame para o S3 em formato Parquet e registra no catálogo do Glue
wr.s3.to_parquet(
    df=df_pandas,
    path=output_s3_path,
    dataset=True,
    mode="overwrite",
    database=database_name,
    table=table_name,
    index=False  # Não inclui o índice do DataFrame como uma coluna no Parquet
)

# Finaliza o job do Glue
job.commit()
