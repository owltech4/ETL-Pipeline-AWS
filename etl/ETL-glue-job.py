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

# Renomeia as colunas do DataFrame do Spark para que sejam iguais aos nomes das colunas do DataFrame do Pandas
#df_spark_renamed = (df_spark
#                    .withColumnRenamed('_p0_id_', 'NewColumn1')
#                    .withColumnRenamed('_p1_a_idade_', 'NewColumn2')
#                    .withColumnRenamed('_p1_a_a_faixa_idade_', 'NewColumn3')
#                    .withColumnRenamed('_p1_b_genero_', 'NewColumn4')
#                    .withColumnRenamed('_p1_e_estado_onde_mora_', 'NewColumn5')
#                    .withColumnRenamed('_p1_e_a_uf_onde_mora_', 'NewColumn6')
#                    .withColumnRenamed('_p1_e_b_regiao_onde_mora_', 'NewColumn7')
#                    .withColumnRenamed('_p1_g_b_regiao_de_origem_', 'NewColumn8')
#                    .withColumnRenamed('_p1_g_c_mudou_de_estado_', 'NewColumn9')
#                    .withColumnRenamed('_p1_h_nivel_de_ensino_', 'NewColumn10')
#                    .withColumnRenamed('_p1_i_area_de_formacao_', 'NewColumn11')
#                    .withColumnRenamed('_p2_a_qual_sua_situacao_atual_de_trabalho_', 'NewColumn12')
#                    .withColumnRenamed('_p2_b_setor_', 'NewColumn13')
#                    .withColumnRenamed('_p2_c_numero_de_funcionarios_', 'NewColumn14')
#                    .withColumnRenamed('_p2_d_gestor_', 'NewColumn15')
                    # Continue com as demais colunas conforme necessário
                   )

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
