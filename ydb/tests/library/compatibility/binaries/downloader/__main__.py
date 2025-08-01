import os
import stat
import sys

import boto3
from botocore import UNSIGNED
from botocore.client import Config

AWS_ENDPOINT = "https://storage.yandexcloud.net"
AWS_BUCKET = "ydb-builds"


def main():
    mode = sys.argv[1]
    if mode == 'download':
        s3_client = boto3.client(
            service_name="s3",
            endpoint_url=AWS_ENDPOINT,
            config=Config(signature_version=UNSIGNED)
        )
        s3_bucket = AWS_BUCKET
        remote_src = sys.argv[2]
        local_dst = sys.argv[3]
        binary_name = sys.argv[4] if len(sys.argv) > 4 else None
        s3_client.download_file(s3_bucket, remote_src, local_dst)

        # chmod +x
        st = os.stat(local_dst)
        os.chmod(local_dst, st.st_mode | stat.S_IEXEC)
        if binary_name:
            with open(local_dst + "-name", "w") as f:
                f.write(binary_name)
    elif mode == 'append-version':
        local_dst = sys.argv[2]
        binary_name = sys.argv[3]
        with open(local_dst, "w") as f:
            f.write(binary_name)


if __name__ == "__main__":
    main()
