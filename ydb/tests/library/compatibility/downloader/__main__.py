import os
import stat
import sys

import boto3
from botocore import UNSIGNED
from botocore.client import Config

AWS_ENDPOINT = "https://storage.yandexcloud.net"
AWS_BUCKET = "ydb-builds"

def download_files(s3_client, bucket, pairs):
    for remote_src, local_dst in pairs:
        s3_client.download_file(bucket, remote_src, local_dst)
        # chmod +x
        st = os.stat(local_dst)
        os.chmod(local_dst, st.st_mode | stat.S_IEXEC)

def main():
    if len(sys.argv) % 2 != 1:
        print("Usage: python __main__.py <remote_src1> <local_dst1> <remote_src2> <local_dst2> ...")
        sys.exit(1)

    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=AWS_ENDPOINT,
        config=Config(signature_version=UNSIGNED)
    )

    s3_bucket = AWS_BUCKET
    pairs = list(zip(sys.argv[1::2], sys.argv[2::2]))

    download_files(s3_client, s3_bucket, pairs)

if __name__ == "__main__":
    main()
