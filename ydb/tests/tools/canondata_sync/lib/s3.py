from typing import Set

import boto3
import requests

from .utils import MDS_PREFIX


def s3_get_bucket(aws_access_key_id, aws_secret_access_key, bucket_name, endpoint="https://storage.yandexcloud.net"):
    return boto3.resource(
        service_name="s3",
        endpoint_url=endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    ).Bucket(bucket_name)


def s3_copy_file(s3_bucket, url: str, key: str):
    data = requests.get(url, stream=True)

    content_type = data.headers["Content-Type"]
    size = data.headers.get("Content-Length")

    print(f"upload {key}, content_type={content_type}, size={size}")

    s3_bucket.upload_fileobj(
        Key=key,
        # FIXME: is it ok to use .raw here? Should we pass content-encoding or it always unset?
        Fileobj=data.raw,
        ExtraArgs=dict(
            ACL="public-read",
            ContentType=content_type,
        ),
    )


def s3_list_objects(s3_bucket) -> Set[str]:
    return {o.key for o in s3_bucket.objects.all()}


def do_sync(s3_bucket, found_objects: Set[str]):
    exists_objects = s3_list_objects(s3_bucket)

    to_upload = [key for key in found_objects if key not in exists_objects]

    print(f"summary: remote={len(exists_objects)}, upload={len(to_upload)}")

    for key in to_upload:
        url = MDS_PREFIX + key
        s3_copy_file(s3_bucket, url, key)

    print("done!")
