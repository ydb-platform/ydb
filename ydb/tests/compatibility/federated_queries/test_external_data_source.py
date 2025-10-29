# -*- coding: utf-8 -*-
import boto3
import pytest
import yatest
import os

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture

from ydb.export import ExportToS3Settings


class ExternalDataTableTestBase:
    def setup_cluster(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        self.s3_config = self.setup_s3()
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config
        self.settings = (
            ExportToS3Settings()
            .with_endpoint(s3_endpoint)
            .with_access_key(s3_access_key)
            .with_secret_key(s3_secret_key)
            .with_bucket(s3_bucket)
        )

        yield from super().setup_cluster(
            extra_feature_flags={
                "enable_external_data_sources": True,
            }
        )

    @staticmethod
    def setup_s3():
        s3_endpoint = os.getenv("S3_ENDPOINT")
        s3_access_key = "minio"
        s3_secret_key = "minio123"
        s3_bucket = "test_bucket"

        resource = boto3.resource(
            "s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
        )

        bucket = resource.Bucket(s3_bucket)
        bucket.create()
        bucket.objects.all().delete()
        bucket.put_object(Key="file.txt", Body="Hello S3!")

        return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket

    def create_external_data_source(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE OBJECT s3_access_key (TYPE SECRET) WITH value="{s3_access_key}";
                CREATE OBJECT s3_secret_key (TYPE SECRET) WITH value="{s3_secret_key}";
            """
            session_pool.execute_with_retries(query)

            query = f"""
                CREATE EXTERNAL DATA SOURCE s3_source WITH (
                    SOURCE_TYPE = "ObjectStorage",
                    LOCATION = "{s3_endpoint}/{s3_bucket}",
                    AUTH_METHOD="AWS",
                    AWS_ACCESS_KEY_ID_SECRET_NAME="s3_access_key",
                    AWS_SECRET_ACCESS_KEY_SECRET_NAME="s3_secret_key",
                    AWS_REGION="us-east-1"
                );
            """
            session_pool.execute_with_retries(query)

    def do_test_external_data(self):
        s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = """
                SELECT * FROM s3_source.`file.txt` WITH (
                    FORMAT = "raw",
                    SCHEMA = ( Data String )
                );
            """
            result_sets = session_pool.execute_with_retries(query)
            data = result_sets[0].rows[0]['Data']
            assert isinstance(data, bytes) and data.decode() == 'Hello S3!'


class TestExternalDataTableMixedCluster(ExternalDataTableTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        self.do_test_external_data()


class TestExternalDataTableRestartToAnotherVersion(ExternalDataTableTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        self.change_cluster_version()
        self.do_test_external_data()


class TestExternalDataTableRollingUpgradeAndDowngrade(ExternalDataTableTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        for _ in self.roll():
            self.do_test_external_data()
