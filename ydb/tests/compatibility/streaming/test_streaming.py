# -*- coding: utf-8 -*-
import pytest
import yatest
import os

from ydb.tests.oss.ydb_sdk_import import ydb
# from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture


class StreamingTestBase(TestYdsBase):
    def setup_cluster(self):
        
        if min(self.versions) < (25, 4):
            pytest.skip("Only available since 25-1")

        output_path = yatest.common.test_output_path()
        self.output_f = open(os.path.join(output_path, "out.log"), "w")
        # self.s3_config = self.setup_s3()
        # s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config
        # self.settings = (
        #     ExportToS3Settings()
        #     .with_endpoint(s3_endpoint)
        #     .with_access_key(s3_access_key)
        #     .with_secret_key(s3_secret_key)
        #     .with_bucket(s3_bucket)
        # )

        yield from super().setup_cluster(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True
            }
        )

    # @staticmethod
    # def setup_s3():
    #     s3_endpoint = os.getenv("S3_ENDPOINT")
    #     s3_access_key = "minio"
    #     s3_secret_key = "minio123"
    #     s3_bucket = "test_bucket"

    #     resource = boto3.resource(
    #         "s3", endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
    #     )

    #     bucket = resource.Bucket(s3_bucket)
    #     bucket.create()
    #     bucket.objects.all().delete()
    #     bucket.put_object(Key="file.txt", Body="Hello S3!")

    #     return s3_endpoint, s3_access_key, s3_secret_key, s3_bucket

    def create_external_data_source(self):
        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TOPIC `{self.input_topic}`;
                CREATE TOPIC `{self.output_topic}`;
            """
            session_pool.execute_with_retries(query)

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE EXTERNAL DATA SOURCE source_name WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{self.endpoint}",
                    DATABASE_NAME="{self.database_path}",
                    SHARED_READING="false",
                    AUTH_METHOD="NONE");
            """
            session_pool.execute_with_retries(query)

    def create_streaming_query(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY query_name AS
                DO BEGIN
                    $in = SELECT time FROM source_name.`{self.input_topic}`
                    WITH (
                        FORMAT="json_each_row",
                        SCHEMA=(time String NOT NULL))
                    WHERE time like "%lunch%";
                    INSERT INTO source_name.`{self.output_topic}` SELECT time FROM $in;
                END DO;
            """
            session_pool.execute_with_retries(query)

    def do_test(self):
        pass
        # s3_endpoint, s3_access_key, s3_secret_key, s3_bucket = self.s3_config

        # with ydb.QuerySessionPool(self.driver) as session_pool:
        #     query = """
        #         SELECT * FROM s3_source.`file.txt` WITH (
        #             FORMAT = "raw",
        #             SCHEMA = ( Data String )
        #         );
        #     """
        #     result_sets = session_pool.execute_with_retries(query)
        #     data = result_sets[0].rows[0]['Data']
        #     assert isinstance(data, bytes) and data.decode() == 'Hello S3!'


# class TestExternalDataTableMixedCluster(StreamingTestBase, MixedClusterFixture):
#     @pytest.fixture(autouse=True, scope="function")
#     def setup(self):
#         yield from self.setup_cluster()

#     def test_external_data_source(self):
#         self.create_external_data_source()
#         self.do_test_external_data()


class TestExternalDataTableRestartToAnotherVersion(StreamingTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        self.create_streaming_query()
        self.change_cluster_version()
        self.do_test()


# class TestExternalDataTableRollingUpgradeAndDowngrade(StreamingTestBase, RollingUpgradeAndDowngradeFixture):
#     @pytest.fixture(autouse=True, scope="function")
#     def setup(self):
#         yield from self.setup_cluster()

#     def test_external_data_source(self):
#         self.create_external_data_source()
#         for _ in self.roll():
#             self.do_test_external_data()
