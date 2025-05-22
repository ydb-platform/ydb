import pytest
from library.recipes import common as recipes_common
import yatest.common
import moto
import requests
import logging
import os
import boto3
import hashlib


class S3Base(object):

    @classmethod
    def s3base_setup_classmethod(cls, s3_endpoint):
        cls.s3_endpoint_url = s3_endpoint

    @pytest.fixture(scope="module", autouse=True)
    def s3base_setup(self):
        s3_pid_file = "s3.pid"
        moto_server_path = os.environ["MOTO_SERVER_PATH"]

        port_manager = yatest.common.network.PortManager()
        s3_port = port_manager.get_port()
        s3_endpoint = "http://localhost:{port}".format(port=s3_port)
        self.s3base_setup_classmethod(s3_endpoint)

        command = [yatest.common.binary_path(moto_server_path), "s3", "--host", "::1", "--port", str(s3_port)]

        def is_s3_ready():
            try:
                response = requests.get(s3_endpoint)
                response.raise_for_status()
                return True
            except requests.RequestException as err:
                logging.debug(err)
                return False

        recipes_common.start_daemon(
            command=command, environment=None, is_alive_check=is_s3_ready, pid_file_name=s3_pid_file
        )

        mock_s3 = moto.mock_s3()
        mock_s3.start()

        yield

        mock_s3.stop()

        with open(s3_pid_file, 'r') as f:
            pid = int(f.read())
        recipes_common.stop_daemon(pid)

    @classmethod
    def s3_session_client(cls):
        session = boto3.session.Session()
        return session.client(
            service_name='s3',
            aws_access_key_id=cls.s3_access_key(),
            aws_secret_access_key=cls.s3_secret_access_key(),
            region_name=cls.s3_region_name(),
            endpoint_url=cls.s3_endpoint())

    @classmethod
    def s3_region_name(cls):
        return 'us-east-1'

    @classmethod
    def s3_access_key(cls):
        return "testing"

    @classmethod
    def s3_secret_access_key(cls):
        return "testing"

    @classmethod
    def s3_endpoint(cls):
        return cls.s3_endpoint_url

    @pytest.fixture(scope="function", autouse=True)
    def s3_base_setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.bucket = "bucket_" + hashlib.md5(S3Base.replace_special_chars(current_test_full_name).encode()).hexdigest()

    @staticmethod
    def replace_special_chars(input_string):
        # Forbidden characters
        characters_to_replace = ":() ."

        # Replace any forbidden characters with '_'
        result = ''.join('_' if char in characters_to_replace else char for char in input_string)

        return result.lower()

    def bucket_name(self):
        return self.bucket

    def create_external_datasource_and_secrets(self, bucket_name):
        self.query(f"""
                CREATE OBJECT `{self.table_path}_id` (TYPE SECRET) WITH (value=`{self.s3_access_key()}`);
                CREATE OBJECT `{self.table_path}_key` (TYPE SECRET) WITH (value=`{self.s3_secret_access_key()}`);
                   """)

        self.query(f"""
        CREATE EXTERNAL DATA SOURCE `{self.table_path}_external_datasource` WITH (
            SOURCE_TYPE="ObjectStorage",
            LOCATION='{self.s3_endpoint()}/{bucket_name}',
            AUTH_METHOD="AWS",
            AWS_ACCESS_KEY_ID_SECRET_NAME="{self.table_path}_id",
            AWS_SECRET_ACCESS_KEY_SECRET_NAME="{self.table_path}_key",
            AWS_REGION="{self.s3_region_name()}"
        );""")
