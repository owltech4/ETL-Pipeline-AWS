## ETL-Pipeline-AWS
ETL Pipeline with AWS and Python

This is a projecto aim to develop an Extract, Transform, Load (ETL) pipeline that gathers data from various sources, transforms it into a useful format, and loads it into a database. Use AWS services like AWS CLI to upload a data into bucket S3, AWS Glue for automation, S3 for storage, Athena to manipulate the final data.

0. Configure the AWS CLI
First of all you must install the AWS CLI and use AWS SDK for Python (boto3) in your computer to be able to integrate as resources


1. Create the S3 bucket
Create a template to create a S3 bucket, using IaC (Infra as Code)

Once create a template it's importante test it. You can test your template in AWS CLI, follows the code example:
aws cloudformation validate-template --template-body file://C://Users/Desktop/Desktop/Estudo/ETL-Pipeline-AWS/IaC/create-s3-bucket.yaml

And to create a stack in AWS is:
aws cloudformation create-stack --stack-name meu-stack-s3 --template-body file://C://Users/Desktop/Desktop/Estudo/ETL-Pipeline-AWS/IaC/create-s3-bucket.yaml


2. Upload the file to created S3 bucket
With the created bucket you can send your local data to the bucket


3. Configure the IAM Roles and User



4. Configure the ETL Glue job



5. Create the script



6. Create a bucket to act as a repository for Athena



7. Explore your data 
