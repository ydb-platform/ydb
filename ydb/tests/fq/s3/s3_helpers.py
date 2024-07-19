#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
import boto3

import ydb.tests.library.common.yatest_common as yatest_common


@dataclass
class S3:
    s3_url: str


def create_bucket_and_upload_file(filename, s3_url, bucket_name, base_path):
    resource = boto3.resource("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")

    bucket = resource.Bucket(bucket_name)
    bucket.create(ACL='public-read')
    bucket.objects.all().delete()

    s3_client = boto3.client("s3", endpoint_url=s3_url, aws_access_key_id="key", aws_secret_access_key="secret_key")
    s3_client.upload_file(yatest_common.source_path("{}/{}".format(base_path, filename)), bucket_name, filename)
