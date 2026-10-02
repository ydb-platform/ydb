#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import boto3

import yatest


@dataclass
class S3:
    s3_url: str


def create_bucket_and_upload_file(filename, s3_url, bucket_name, base_path):
    resource = boto3.resource("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")

    bucket = resource.Bucket(bucket_name)
    bucket.create(ACL='public-read')
    bucket.objects.all().delete()

    s3_client = boto3.client("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")
    s3_client.upload_file(yatest.common.source_path("{}/{}".format(base_path, filename)), bucket_name, filename)


def create_bucket_and_put_object(s3_url, bucket_name, object_key, body, content_type="text/plain"):
    """Create bucket, clear objects, upload in-memory body (same session layout as create_bucket_and_upload_file)."""
    resource = boto3.resource("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")

    bucket = resource.Bucket(bucket_name)
    bucket.create(ACL='public-read')
    bucket.objects.all().delete()

    s3_client = boto3.client("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body, ContentType=content_type)
