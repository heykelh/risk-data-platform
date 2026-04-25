"""
Client Cloudflare R2 partagé entre tous les scripts d'ingestion.
Compatible avec l'API S3 via boto3.
"""
import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()


def get_r2_client():
    """Retourne un client boto3 configuré pour Cloudflare R2."""
    account_id = os.environ["R2_ACCOUNT_ID"]
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
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