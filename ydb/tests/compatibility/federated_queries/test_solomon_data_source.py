# -*- coding: utf-8 -*-
import boto3
import pytest
import yatest
import os

from ydb.library.yql.tools.solomon_emulator.client.client import cleanup_emulator, add_solomon_metrics
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture


class SolomonDataSourceTestBase:
    def setup_cluster(self):
        if min(self.versions) < (25, 3):
            pytest.skip("Only available since 25-3")
        
        self.solomon_config = self.setup_solomon()

        yield from super().setup_cluster(
            extra_feature_flags={
                "enable_external_data_sources": True,
            },
            query_service_config={
                "solomon": {
                    "default_settings": [
                        {
                            "name": "_EnableReading",
                            "value": "true"
                        },
                        {
                            "name": "_EnableRuntimeListing",
                            "value": "true"
                        },
                        {
                            "name": "_EnableSolomonClientPostApi",
                            "value": "false"
                        }
                    ]
                }
            }
        )

    @staticmethod
    def setup_solomon():
        solomon_http_endpoint = os.environ.get("SOLOMON_HTTP_ENDPOINT")
        solomon_grpc_endpoint = os.environ.get("SOLOMON_GRPC_ENDPOINT")

        cleanup_emulator()

        add_solomon_metrics("compatability_test", "my_cluster", "my_service", {"metrics": [
            {
                "labels"        : {"test_type": "compatability_test"},
                "type"          : "DGAUGE",
                "timestamps"    : [0],
                "values"        : [123]
            }
        ]})

        return solomon_http_endpoint, solomon_grpc_endpoint

    def create_external_data_source(self):
        solomon_http_endpoint, solomon_grpc_endpoint = self.solomon_config

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE EXTERNAL DATA SOURCE solomon_source WITH (
                    SOURCE_TYPE     = "Solomon",
                    LOCATION        = "{solomon_http_endpoint}",
                    GRPC_LOCATION   = "{solomon_grpc_endpoint}",
                    AUTH_METHOD     = "NONE",
                    USE_TLS         = "false"
                );
            """
            session_pool.execute_with_retries(query)

    def do_test_external_data(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = """
            SELECT * FROM solomon_source.compatability_test WITH (
                program = @@{cluster="my_cluster", service="my_service", test_type="compatability_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            );
            """
            result_sets = session_pool.execute_with_retries(query)
            data = result_sets[0].rows[0]['value']
            assert isinstance(data, float) and int(data) == 123

            query = """
            SELECT * FROM solomon_source.compatability_test WITH (
                selectors = @@{cluster="my_cluster", service="my_service", test_type="compatability_test"}@@,

                from = "1970-01-01T00:00:00Z",
                to = "1970-01-01T00:01:00Z"
            );
            """
            result_sets = session_pool.execute_with_retries(query)
            data = result_sets[0].rows[0]['value']
            assert isinstance(data, float) and int(data) == 123


class TestSolomonDataSourceMixedCluster(SolomonDataSourceTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        self.do_test_external_data()


class TestSolomonDataSourceRestartToAnotherVersion(SolomonDataSourceTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        self.change_cluster_version()
        self.do_test_external_data()


class TestSolomonDataSourceRollingUpgradeAndDowngrade(SolomonDataSourceTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_external_data_source(self):
        self.create_external_data_source()
        for _ in self.roll():
            self.do_test_external_data()
