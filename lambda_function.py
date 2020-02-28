from __future__ import absolute_import, print_function, unicode_literals
from io import BytesIO
from gzip import GzipFile
import urllib
import os

import boto3
s3 = boto3.client('s3')
bucket = 'lambda-testing-py'

def lambda_handler(event, context):
    #get object
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    #get filename from object
    filename = os.path.splitext(str(key))[0]
    #retrive object from s3 bucket
    try:
        retr = s3.get_object(Bucket=bucket, Key=key)
        bytestream = BytesIO(retr['Body'].read())
        gzf = GzipFile(None, 'rb', fileobj=bytestream)
        #read data from .gz file
        data = gzf.read().decode('utf-8')
        #put file to s3 bucket
        s3.put_object(Bucket=bucket,Key=filename,Body=data)
        #delete .gz file
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    
