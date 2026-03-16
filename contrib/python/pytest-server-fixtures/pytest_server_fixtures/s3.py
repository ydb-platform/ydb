# coding: utf-8
"""
Pytest fixtures to launch a minio S3 server and get a bucket for it.
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import uuid
from collections import namedtuple
import logging
import os

import pytest
from future.utils import text_type
from pytest_fixture_config import requires_config

from . import CONFIG
from .http import HTTPTestServer

log = logging.getLogger(__name__)

def _s3_server(request):
    server = MinioServer()
    server.start()
    request.addfinalizer(server.teardown)
    return server


@pytest.fixture(scope="session")
@requires_config(CONFIG, ['minio_executable'])
def s3_server(request):
    """
    Creates a session-scoped temporary S3 server using the 'minio' tool.

    The primary method on the server object is `s3_server.get_s3_client()`, which returns a boto3 `Resource`
    (`boto3.resource('s3', ...)`)
    """
    return _s3_server(request)

BucketInfo = namedtuple('BucketInfo', ['client', 'name'])
# Minio is a little too slow to start for each function call
# Start it once per session and get a new bucket for each function instead.
@pytest.fixture(scope="function")
def s3_bucket(s3_server):  # pylint: disable=redefined-outer-name
    """
    Creates a function-scoped s3 bucket,
    returning a BucketInfo namedtuple with `s3_bucket.client` and `s3_bucket.name` fields
    """
    client = s3_server.get_s3_client()
    bucket_name = text_type(uuid.uuid4())
    client.create_bucket(Bucket=bucket_name)
    return BucketInfo(client, bucket_name)


class MinioServer(HTTPTestServer):
    random_port = True
    aws_access_key_id = "MINIO_TEST_ACCESS"
    aws_secret_access_key = "MINIO_TEST_SECRET"

    def __init__(self, workspace=None, delete=None, preserve_sys_path=False, **kwargs):
        env = kwargs.get('env', os.environ.copy())
        env.update({"MINIO_ACCESS_KEY": self.aws_access_key_id, "MINIO_SECRET_KEY": self.aws_secret_access_key})
        kwargs['env'] = env
        kwargs['hostname'] = "0.0.0.0" # minio doesn't seem to allow binding to 127.0.0.0/8
        super(MinioServer, self).__init__(workspace=workspace, delete=delete, preserve_sys_path=preserve_sys_path, **kwargs)

    def get_s3_client(self):
        # Region name and signature are to satisfy minio
        import boto3
        import botocore.client
        s3 = boto3.resource(
            's3',
            endpoint_url=self.boto_endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name='us-east-1',
            config=botocore.client.Config(signature_version='s3v4'),
        )
        return s3

    @property
    def datadir(self):
        return self.workspace / 'minio-db'

    @property
    def boto_endpoint_url(self):
        return self.uri

    def pre_setup(self):
        self.datadir.mkdir()  # pylint: disable=no-value-for-parameter

    @property
    def run_cmd(self):
        cmdargs = [
            CONFIG.minio_executable,
            "server",
            "--address",
            "{}:{}".format(self.hostname, self.port),
            text_type(self.datadir),
        ]
        return cmdargs
