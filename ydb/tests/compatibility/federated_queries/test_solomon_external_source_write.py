# -*- coding: utf-8 -*-

import logging
import os
import pytest
import time

from ydb.library.yql.tools.solomon_emulator.client.client import cleanup_solomon, get_solomon_metrics

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class SolomonExternalSourceWriteTestBase:
    SOLOMON_PROJECT = "compat_solomon_project"
    SOLOMON_CLUSTER = "compat_solomon_cluster"
    SOLOMON_SERVICE = "compat_solomon_service"
    SOURCE_NAME = "solomon_source"
    TIMESTAMP_LITERAL = "2026-05-14T09:46:43.275002Z"

    def setup_cluster(self):
        logger.debug(f"setup_cluster, versions {self.versions}")

        if min(self.versions) < (26, 1):
            logger.debug("skip test, only available since 26-1")
            pytest.skip("Only available since 26-1")

        cleanup_solomon(self.SOLOMON_PROJECT, self.SOLOMON_CLUSTER, self.SOLOMON_SERVICE)

        yield from super().setup_cluster(
            extra_feature_flags={
                "enable_external_data_sources": True,
            },
            query_service_config={
                "available_external_data_sources": ["Solomon"],
            },
            additional_log_configs={
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG},
        )

    def create_external_data_source(self):
        logger.debug("create_external_data_source")
        solomon_http_endpoint = os.environ["SOLOMON_HTTP_ENDPOINT"]
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE EXTERNAL DATA SOURCE {self.SOURCE_NAME} WITH (
                    SOURCE_TYPE = "Solomon",
                    LOCATION = "{solomon_http_endpoint}",
                    AUTH_METHOD = "NONE",
                    USE_TLS = "false"
                );
            """
            session_pool.execute_with_retries(query)
        self.output_object = f"`{self.SOURCE_NAME}`.`{self.SOLOMON_PROJECT}/{self.SOLOMON_CLUSTER}/{self.SOLOMON_SERVICE}`"

    def do_test_part(self, sensor_value: int):
        logger.debug(f"write data to solomon with sensor={sensor_value}")
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                INSERT INTO {self.output_object}
                SELECT
                    Unwrap(CAST("{self.TIMESTAMP_LITERAL}" AS Timestamp)) AS Ts,
                    "my_data" AS Label,
                    {sensor_value} AS Sensor;
            """
            session_pool.execute_with_retries(query)

        logger.debug("read metrics from solomon emulator")
        metrics = get_solomon_metrics(self.SOLOMON_PROJECT, self.SOLOMON_CLUSTER, self.SOLOMON_SERVICE)
        assert metrics, f"No metrics found for {self.SOLOMON_PROJECT}/{self.SOLOMON_CLUSTER}/{self.SOLOMON_SERVICE}"
        values = [int(m["value"]) for m in metrics]
        assert sensor_value in values, f"Sensor value {sensor_value} not found in metrics: {metrics}"


class TestSolomonExternalSourceWriteMixedCluster(SolomonExternalSourceWriteTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_mixed_cluster(self):
        self.create_external_data_source()
        self.do_test_part(sensor_value=42)
        self.do_test_part(sensor_value=43)


class TestSolomonExternalSourceWriteRestartToAnotherVersion(SolomonExternalSourceWriteTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_restart_to_another_version(self):
        self.create_external_data_source()
        self.do_test_part(sensor_value=42)
        self.change_cluster_version()
        self.do_test_part(sensor_value=43)


class TestSolomonExternalSourceWriteRollingUpgradeAndDowngrade(SolomonExternalSourceWriteTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_rolling_upgrade(self):
        self.create_external_data_source()

        for i, _ in enumerate(self.roll()):
            self.do_test_part(sensor_value=42 + i)
            time.sleep(0.5)
