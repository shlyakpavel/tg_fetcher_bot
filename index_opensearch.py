import os
from datetime import datetime, timezone

import boto3
from opensearchpy import OpenSearch
from dotenv import load_dotenv

load_dotenv()

S3_REGION = os.getenv("S3_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST")
OPENSEARCH_INDEX = "documents"
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD")

s3_client = boto3.client(
    's3',
	endpoint_url=f"https://{S3_REGION}.digitaloceanspaces.com",
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

opensearch_client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=False,
	port=25060
)

def index_file(file_key):
    file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
    content = file_obj["Body"].read().decode("utf-8")

    doc = {
        "filename": file_key,
        "content": content,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    response = opensearch_client.index(index=OPENSEARCH_INDEX, body=doc)
    print(f"Indexed {file_key}: {response}")

def list_and_index():
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET)

    if "Contents" in response:
        for obj in response["Contents"]:
            file_key = obj["Key"]
            if file_key.endswith(".txt"):
                print(f"Indexing {file_key}...")
                index_file(file_key)

if __name__ == "__main__":
    list_and_index()
