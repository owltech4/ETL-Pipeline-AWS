# ETL-Pipeline-AWS
ETL Pipeline with AWS and Python

Develop an Extract, Transform, Load (ETL) pipeline that gathers data from various sources, transforms it into a useful format, and loads it into a database. Use AWS services like AWS Glue for automation and S3 for storage.

Importante testar o template criado de criação do bucket S3. O comando no AWS CLI para testar é:
aws cloudformation validate-template --template-body file://C://Users/Desktop/Desktop/Estudo/ETL-Pipeline-AWS/IaC/create-s3-bucket.yaml

E para criar o stack na AWS:
aws cloudformation create-stack --stack-name meu-stack-s3 --template-body file://C://Users/Desktop/Desktop/Estudo/ETL-Pipeline-AWS/IaC/create-s3-bucket.yaml
