"""
Client MinIO/S3 partagé entre tous les scripts d'ingestion.
Compatible MinIO local et Cloudflare R2.
"""
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()


def get_r2_client():
    """Retourne un client boto3 configuré pour MinIO ou Cloudflare R2."""
    endpoint = os.environ["R2_ENDPOINT_URL"]  # http://localhost:9000 en local
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_json(client, data: bytes, key: str) -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/json",
    )


def upload_parquet(client, data: bytes, key: str) -> None:
    bucket = os.environ["R2_BUCKET_NAME"]
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
    )