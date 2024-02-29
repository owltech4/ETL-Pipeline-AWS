import boto3 #AWS SDK for Python
from datetime import datetime
import os  # Import the os module


def upload_file_to_s3(file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use just the file name (not the entire path)
    if object_name is None:
        object_name = os.path.basename(file_name)  # Extracts just the file name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        start_time = datetime.now()
        print(f"Upload started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        response = s3_client.upload_file(file_name, bucket_name, object_name)
        
        finish_time = datetime.now()
        print(f"Upload finished at {finish_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Calculate and print the duration
        print(f"Total upload time: {finish_time - start_time}")
    except Exception as e:
        print(f"Upload failed: {e}")
        return False
    return True


# Example usage
file_name = 'C://Users/Desktop/Desktop/Estudo/ETL-Pipeline-AWS/dataset/State of Data 2021 - Dataset - Pgina1.csv'
bucket_name = 'bucket-study-marcelo' # Just the bucket name, without 's3://'

# Call the function to upload your file
success = upload_file_to_s3(file_name, bucket_name)

if success:
    print("File uploaded successfully.")
else:
    print("File upload failed.")
