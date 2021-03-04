import os
import boto3
import botocore
import pandas as pd

def push_local_data_to_s3(local_filenames, local_dir, s3_path, s3_filenames=None, s3_bucket='nate-givens-ds'):
    if s3_filenames = None:
        s3_filenames = local_filenames
    for file in local_filenames:
        print(file)