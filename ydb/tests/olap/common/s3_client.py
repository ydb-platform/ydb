import logging
import requests
from typing import Tuple, Optional

import boto3
import yatest.common
from library.recipes import common as recipes_common


class S3Mock:
    def __init__(self, moto_server_path: str):
        self.moto_server_path = moto_server_path
        self.s3_pid_file = "s3.pid"
        self.port_manager = yatest.common.network.PortManager()

        self.endpoint: Optional[str] = None
        self.s3_pid: Optional[int] = None

    def __is_s3_ready(self):
        assert self.endpoint is not None, "expected filled endpoint"

        try:
            response = requests.get(self.endpoint)
            response.raise_for_status()
            return True
        except requests.RequestException as err:
            logging.debug(err)
            return False

    def start(self):
        assert self.s3_pid is None, "s3 already started"

        port = self.port_manager.get_port()
        self.endpoint = f"http://localhost:{port}"
        command = [self.moto_server_path, "s3", "--port", str(port)]

        recipes_common.start_daemon(
            command=command, environment=None, is_alive_check=self.__is_s3_ready, pid_file_name=self.s3_pid_file
        )

        with open(self.s3_pid_file, 'r') as s3_pid_file:
            self.s3_pid = int(s3_pid_file.read())

    def stop(self):
        assert self.s3_pid is not None, "s3 is not started"
        recipes_common.stop_daemon(self.s3_pid)
        self.s3_pid = None


class S3Client:
    def __init__(self, endpoint: str, region: str = "us-east-1", key_id: str = "fake_key_id", key_secret: str = "fake_key_secret"):
        self.endpoint = endpoint
        self.region = region
        self.key_id = key_id
        self.key_secret = key_secret

        session = boto3.session.Session()
        self.s3 = session.resource(
            service_name="s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=key_secret,
            region_name=region,
            endpoint_url=endpoint
        )
        self.client = session.client(
            service_name="s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=key_secret,
            region_name=region,
            endpoint_url=endpoint
        )

    def create_bucket(self, name: str):
        self.client.create_bucket(Bucket=name)

    def get_bucket_stat(self, bucket_name: str) -> Tuple[int, int]:
        bucket = self.s3.Bucket(bucket_name)
        count = 0
        size = 0
        for obj in bucket.objects.all():
            count += 1
            size += obj.size
        return (count, size)
