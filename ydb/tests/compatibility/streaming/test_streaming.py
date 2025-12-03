# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_watermarks(request):
    return getattr(request, 'param', True)


class StreamingTestBase:
    def setup_cluster(self, enable_watermarks: bool):
        logger.debug(f"setup_cluster, versions {self.versions}, {enable_watermarks=}")

        if min(self.versions) < (25, 4):
            logger.debug("skip test, only available since 25-4")
            pytest.skip("Only available since 25-4")

        os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = "200"
        os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "15"

        erasure = None if enable_watermarks else Erasure.MIRROR_3_DC  # TODO: Erasure.MIRROR_3_DC
        port_allocator = KikimrPortManagerPortAllocator()
        query_service_config = {
            "streaming_queries": {
                "external_storage": {
                    "database_connection": {
                        "endpoint": f"localhost:{port_allocator.get_node_port_allocator(1).grpc_port}",
                        "database": "/Root",
                    },
                },
            },
        } if enable_watermarks else None

        yield from super().setup_cluster(
            erasure=erasure,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True,
            },
            port_allocator=port_allocator,
            query_service_config=query_service_config,
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.TRACE,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.TRACE,
                'STREAMS_STORAGE_SERVICE': LogLevels.TRACE,
                'FQ_ROW_DISPATCHER': LogLevels.TRACE,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG,
            },
            table_service_config={
                "enable_watermarks": enable_watermarks,
            },
        )

    def create_topics(self):
        logger.debug("create_topics")
        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        self.consumer_name = 'consumer_name'
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TOPIC `{self.input_topic}`;
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
            """
            session_pool.execute_with_retries(query)

    def create_external_data_source(self, enable_watermarks: bool):
        logger.debug(f"create_external_data_source, {enable_watermarks=}")

        endpoint = f"localhost:{self.cluster.nodes[1].port}"
        shared_reading = str(enable_watermarks).lower()
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE EXTERNAL DATA SOURCE source_name WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{endpoint}",
                    DATABASE_NAME="{self.database_path}",
                    SHARED_READING="{shared_reading}",
                    AUTH_METHOD="NONE"
                );
            """
            session_pool.execute_with_retries(query)

    def create_streaming_query(self, enable_watermarks: bool):
        logger.debug(f"create_streaming_query, {enable_watermarks=}")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            max_tasks_per_stage = 'PRAGMA ydb.MaxTasksPerStage = "1";' if enable_watermarks else ""
            watermarks = ', WATERMARK AS (CAST(time AS Timestamp) - Interval("PT1M"))' if enable_watermarks else ""
            query = f"""
                CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
                {max_tasks_per_stage}
                $input = (
                    SELECT * FROM
                        source_name.`{self.input_topic}` WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                            {watermarks}
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT host, COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S'),
                        host
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
                    FROM $number_errors
                );

                INSERT INTO source_name.`{self.output_topic}`
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_simple_streaming_query(self, enable_watermarks: bool):
        logger.debug(f"create_simple_streaming_query, {enable_watermarks=}")

        with ydb.QuerySessionPool(self.driver) as session_pool:
            max_tasks_per_stage = 'PRAGMA ydb.MaxTasksPerStage = "1";' if enable_watermarks else ""
            watermarks = ', WATERMARK AS (CAST(time AS Timestamp) - Interval("PT1M"))' if enable_watermarks else ""
            query = f"""
                CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
                {max_tasks_per_stage}
                $input = (
                    SELECT
                        *
                    FROM
                        source_name.`{self.input_topic}` WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                            {watermarks}
                        )
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
                    FROM $input
                );

                INSERT INTO source_name.`{self.output_topic}`
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def do_write_read(self, input, expected_output):
        logger.debug("do_write_read")
        endpoint = f"localhost:{self.cluster.nodes[1].port}"
        time.sleep(2)
        logger.debug("write data to stream")
        write_stream(path=self.input_topic, data=input, database=self.database_path, endpoint=endpoint)
        logger.debug("read data from stream")
        assert sorted(read_stream(
            path=self.output_topic,
            messages_count=len(expected_output),
            consumer_name=self.consumer_name,
            database=self.database_path,
            endpoint=endpoint)) == sorted(expected_output)

    def do_test_part1(self):
        input = [
            '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-1"}']
        expected_data = sorted([
            '{"error_count":1,"host":"host-2","ts":"2025-01-01T00:00:00Z"}',
            '{"error_count":2,"host":"host-1","ts":"2025-01-01T00:00:00Z"}'])
        self.do_write_read(input, expected_data)

    def do_test_part2(self):
        input = [
            '{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-2"}']
        expected_data = sorted([
            '{"error_count":2,"host":"host-2","ts":"2025-01-01T00:10:00Z"}',
            '{"error_count":1,"host":"host-1","ts":"2025-01-01T00:10:00Z"}'])
        self.do_write_read(input, expected_data)


class TestStreamingMixedCluster(StreamingTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, enable_watermarks: bool):
        yield from self.setup_cluster(enable_watermarks)

    @link_test_case("#27924")
    @pytest.mark.parametrize('enable_watermarks', [True, False], indirect=True)
    def test_mixed_cluster(self, enable_watermarks: bool):
        self.create_topics()
        self.create_external_data_source(enable_watermarks)
        self.create_streaming_query(enable_watermarks)
        self.do_test_part1()
        self.do_test_part2()


class TestStreamingRestartToAnotherVersion(StreamingTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, enable_watermarks: bool):
        yield from self.setup_cluster(enable_watermarks)

    @link_test_case("#27924")
    @pytest.mark.parametrize('enable_watermarks', [True, False], indirect=True)
    def test_restart_to_another_version(self, enable_watermarks: bool):
        self.create_topics()
        self.create_external_data_source(enable_watermarks)
        self.create_streaming_query(enable_watermarks)
        self.do_test_part1()
        self.change_cluster_version()
        self.do_test_part2()


class TestStreamingRollingUpgradeAndDowngrade(StreamingTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, enable_watermarks: bool):
        yield from self.setup_cluster(enable_watermarks)

    @link_test_case("#27924")
    @pytest.mark.parametrize('enable_watermarks', [True, False], indirect=True)
    def test_rolling_upgrade(self, enable_watermarks: bool):
        self.create_topics()
        self.create_external_data_source(enable_watermarks)
        self.create_simple_streaming_query(enable_watermarks)

        for _ in self.roll():  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct during rolling upgrade
            #
            input = ['{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-2"}']
            expected_data = ['{"host":"host-2","level":"error","time":"2025-01-01T00:15:00.000000Z"}']
            self.do_write_read(input, expected_data)
