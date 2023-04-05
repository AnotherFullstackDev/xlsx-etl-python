import os

import boto3

target_crawler = os.getenv("TARGET_CRAWLER")
glue = boto3.client("glue")

def handler(event, context):
    print("Running crawler: " + target_crawler)

    glue.start_crawler(Name=target_crawler)