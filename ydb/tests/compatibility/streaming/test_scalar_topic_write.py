# -*- coding: utf-8 -*-

import logging
import pytest
import time

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.tools.datastreams_helpers.data_plane import read_stream

logger = logging.getLogger(__name__)


class ScalarTopicWriteTestBase:
    def setup_cluster(self):
        logger.debug(f"setup_cluster, versions {self.versions}")

        if min(self.versions) < (26, 1):
            logger.debug("skip test, only available since 26-1")
            pytest.skip("Only available since 26-1")

        yield from super().setup_cluster(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_topics_sql_io_operations": True,
            },
            additional_log_configs={
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG},
        )

    def create_topics(self, with_external_data_source: bool):
        logger.debug("create_topics")
        self.output_topic = "streaming_recipe/output_topic"
        self.output_object = f"`{self.output_topic}`"
        self.consumer_name = "consumer_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
            """
            session_pool.execute_with_retries(query)

        if with_external_data_source:
            self.create_external_data_source()
            self.output_object = f"`source_name`.`{self.output_topic}`"

    def create_external_data_source(self):
        logger.debug("create_external_data_source")
        endpoint = f"localhost:{self.cluster.nodes[1].port}"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE EXTERNAL DATA SOURCE source_name WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{endpoint}",
                    DATABASE_NAME="{self.database_path}",
                    AUTH_METHOD="NONE");
            """
            session_pool.execute_with_retries(query)

    def do_test_part(self, suffix: str = ""):
        logger.debug("write data to stream")
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                INSERT INTO {self.output_object} SELECT "my_data{suffix}";
            """
            session_pool.execute_with_retries(query)

        logger.debug("read data from stream")
        read_data = read_stream(
            path=self.output_topic,
            messages_count=1,
            consumer_name=self.consumer_name,
            database=self.database_path,
            endpoint=f"localhost:{self.cluster.nodes[1].port}")
        assert read_data, f"No data read from stream {self.output_topic}"
        if len(read_data) > 1:
            read_data = read_data[-1:]        # deduplication disabled
        assert read_data[0] == f"my_data{suffix}"


class TestScalarTopicWriteMixedCluster(ScalarTopicWriteTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#39456")
    @pytest.mark.parametrize("external", [True, False])
    def test_mixed_cluster(self, external):
        self.create_topics(external)
        self.do_test_part()


class TestScalarTopicWriteRestartToAnotherVersion(ScalarTopicWriteTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#39456")
    @pytest.mark.parametrize("external", [True, False])
    def test_restart_to_another_version(self, external):
        self.create_topics(external)
        self.do_test_part(suffix="_before")
        self.change_cluster_version()
        self.do_test_part(suffix="_after")


class TestScalarTopicWriteRollingUpgradeAndDowngrade(ScalarTopicWriteTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#39456")
    @pytest.mark.parametrize("external", [True, False])
    def test_rolling_upgrade(self, external):
        self.create_topics(external)

        for i, _ in enumerate(self.roll()):
            self.do_test_part(suffix=str(i))
            time.sleep(0.5)
