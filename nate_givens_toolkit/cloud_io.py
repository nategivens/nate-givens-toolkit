import os
import boto3
import botocore
import requests
import pandas as pd

def push_file_to_s3(local_filename, local_dir, bucket_dir, bucket, bucket_filename=None, overwrite=False):
    """Copy a local file to an S3 bucket
    
    :param local_filename: the name of the local file to be pushed
    :param local_dir: the local directory where the file to be pushed can be found
    :param bucket_dir: the bucket directory where the file should be placed
    :param bucket: the bucket where the file should be placed
    :param bucket_filename: the name of the file in the bucket (defaults to local_filename)
    :param overwrite: flag to overwrite the file if it already exists in the bucket (defaults to False)
    """
    if bucket_filename is None:
        bucket_filename = local_filename
    local_file = os.path.join(local_dir, local_filename)
    bucket_file = os.path.join(bucket_dir, bucket_filename)
    if (not file_exists_in_s3(bucket_filename, bucket_dir, bucket)) or overwrite:
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(local_file, bucket, bucket_file)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    else:
        return False

def pull_data_from_url(url_filename, url_path, local_dir, local_filename=None, overwrite=False):
    """Copy a file from URL to local directory
    
    Only writes the file if the file exists remotely and (does not exist locally or overwrite is True)
    Returns True if it writes the file locally, False otherwise.
    
    :param url_filename: the name of the file at the URL to be pulled
    :param url: the url where the target file resides
    :param local_dir: the local directory where the file should be placed
    :param local_filename: the name of the local copy of the file (defaults to url_filename)
    :param overwrite: flag to overwrite local file if it exists
    """
    if local_filename is None:
        local_filename = url_filename
    if (not os.path.isfile(os.path.join(local_dir, local_filename))) or overwrite:
        url_file = url_path + url_filename
        r = requests.get(url_file)
        if r.status_code != 200:
            return False
        else:
            open(os.path.join(local_dir, local_filename), 'wb').write(r.content)
            return True
    else:
        return False
        
def pull_file_from_s3(bucket_filename, bucket_dir, bucket, local_dir, local_filename = None, overwrite=False):
    """Retrieve bucket\bucket_dir\bucket_filename and store as local_dir\local_filename
    
    Skips if file does not exist in bucket. If file exists in bucket, only copies locally if file does not exist locally or overwrite is True.
    
    :param bucket_filename: the name of the file in the bucket
    :param bucket_dir: the path to bucket_filename in the bucket
    :parm local_dir: the path to store the file locally
    :param local_filename: the name of the file when stored locally (defaults to bucket_filename)
    :param bucket: the bucket to pull from (defaults to nate-givens-ds)
    :param overwrite: flag to overwrite a local file (defaults to False)    
    """
    if local_filename is None:
        local_filename = bucket_filename
    if not file_exists_in_s3(bucket_filename, bucket_dir, bucket):
        return False
    if (not os.path.isfile(os.path.join(local_dir, local_filename))) or overwrite:
        source_file = os.path.join(bucket_dir, bucket_filename)
        dest_file = os.path.join(local_dir, local_filename)
        s3 = boto3.client('s3')
        s3.download_file(bucket, source_file, dest_file)
        return True
    else:
        return False

def file_exists_in_s3(bucket_filename, bucket_dir, bucket):
    """Return True if bucket\bucket_dir\bucket_filename exists, otherwise False
    
    :param bucket_filename: the name of the file we're checking the bucket for
    :parm bucket_dir: the path to bucket_filename in the bucket
    :param bucket: the bucket to check (defaults to nate-givens-ds)
    """
    s3 = boto3.resource('s3')
    file = os.path.join(bucket_dir, bucket_filename)
    try:
        s3.Object(bucket, file).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise
    else:
        return True