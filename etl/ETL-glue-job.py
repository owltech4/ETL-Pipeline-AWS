import sys
import boto3
import pandas as pd
import awswrangler as wr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()


# Caminho do seu bucket de entrada
input_s3_path = "s3://bucket-study-marcelo/State of Data 2021 - Dataset - Pgina1.csv"

# Lendo o arquivo do S3
df = spark.read.csv(input_s3_path, header=True)

# Mostrando os resultados
df.show()
