import io
import json
import os
import urllib.parse

import boto3
import pandas as pd

s3 = boto3.client("s3")
target_bucket = os.getenv("TARGET_BUCKET")
target_sheet=os.getenv("TARGET_SHEET")


def handler(event, context):
    print("hello from lambda")

    print("event {}".format(json.dumps(event)))

    bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # print("resulting CSV {}".format(pd.read_excel("etl_test.xlsx", "Sheet1", dtype=str, index_col=None).to_csv()))

    s3_object_content = s3.get_object(Bucket=bucket, Key=object_key).get("Body").read()
    parsed_content = pd.read_excel(s3_object_content, target_sheet, dtype=str, index_col=None)

    print("file content {}".format(parsed_content))

    with io.StringIO() as csv_buffer:
        parsed_content.to_csv(csv_buffer, index=False)

        response = s3.put_object(Bucket=target_bucket, Key=object_key, Body=csv_buffer.getvalue())

        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if 200 <= status_code <= 299:
            print("Upload successful")
        else:
            print("Upload failed")

# if __name__ == "__main__":
#     handler({}, {})
