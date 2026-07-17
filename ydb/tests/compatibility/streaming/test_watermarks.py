# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time
from typing import Generator, Self

from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.fq.streaming_common.common import YdbClient

logger = logging.getLogger(__name__)


class StreamingTestBase:
    def setup_cluster(self: Self) -> Generator[None, None, None]:
        logger.debug(f"setup_cluster, versions {self.versions}")

        if min(self.versions) < (26, 2):
            logger.debug("skip test, only available since 26-2")
            pytest.skip("Only available since 26-2")

        extra_feature_flags = [
            "enable_external_data_sources",
            "enable_streaming_queries",
            "enable_shared_reading_in_streaming_queries",
        ]

        os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = "200"
        os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "15"
        for _ in super().setup_cluster(
            disabled_feature_flags=["enable_drain_on_shutdown"],
            extra_feature_flags=extra_feature_flags,
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.TRACE,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.TRACE,
                'STREAMS_STORAGE_SERVICE': LogLevels.TRACE,
                'FQ_ROW_DISPATCHER': LogLevels.TRACE,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTER': LogLevels.DEBUG,
            },
            table_service_config={
                "enable_watermarks": True,
                "enable_watermarks_advanced": True,
            },
        ):
            self.ydb_client = YdbClient(self.driver)
            try:
                yield
            finally:
                self.ydb_client.stop()

    def change_cluster_version(self: Self) -> None:
        self.ydb_client.stop()
        super().change_cluster_version()
        self.ydb_client = YdbClient(self.driver)

    def create_objects(self: Self, external: bool) -> None:
        logger.debug("create_objects")
        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        self.consumer_name = 'consumer_name'
        self.ydb_client.query(f"""
            CREATE TOPIC `{self.input_topic}`;
            CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
        """)

        self.ydb_client.query("""
            CREATE TABLE table_name (
                key Utf8,
                value Utf8,
                PRIMARY KEY (key)
            );
        """)

        self.ydb_client.query("""
            UPSERT INTO table_name (key, value) VALUES ('key1', 'value1');
        """)

        if external:
            self.create_external_data_source()
            self.input_object = f"`source_name`.`{self.input_topic}`"
            self.output_object = f"`source_name`.`{self.output_topic}`"
        else:
            self.input_object = f"`{self.input_topic}`"
            self.output_object = f"`{self.output_topic}`"

    def create_external_data_source(self: Self) -> None:
        logger.debug("create_external_data_source")
        endpoint = f"localhost:{self.cluster.nodes[1].port}"
        self.ydb_client.create_external_data_source("source_name", endpoint, self.database_path)

    def create_streaming_query(self: Self) -> None:
        logger.debug("create_streaming_query")
        self.ydb_client.query(f"""
            CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
            $precompute_data = SELECT value FROM table_name LIMIT 1;

            $input = (
                SELECT
                    *
                FROM
                    {self.input_object} WITH (
                        FORMAT = 'json_each_row',
                        SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL),
                        WATERMARK = CAST(time AS Timestamp) - Interval('PT1M')
                    )
            );

            $output = (
                SELECT
                    host,
                    COUNT(*) AS error_count,
                    CAST(HOP_START() AS String) AS ts
                FROM
                    $input
                WHERE
                    level == 'error'
                GROUP BY
                    host,
                    HoppingWindow(CAST(time AS Timestamp), 'PT600S', 'PT600S')
            );

            INSERT INTO {self.output_object}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) || Unwrap($precompute_data)
            FROM $output;
            END DO;
        """)

    def create_simple_streaming_query(self: Self) -> None:
        logger.debug("create_simple_streaming_query")
        self.ydb_client.query(f"""
            CREATE STREAMING QUERY `my_queries/query_name` AS DO BEGIN
            $precompute_data = SELECT value FROM table_name LIMIT 1;

            $input = (
                SELECT
                    *
                FROM
                    {self.input_object} WITH (
                        FORMAT = 'json_each_row',
                        SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL),
                        WATERMARK = CAST(time AS Timestamp) - Interval('PT1M')
                    )
            );

            INSERT INTO {self.output_object}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) || Unwrap($precompute_data)
            FROM $input;
            END DO;
        """)

    def do_write_read(self: Self, input_data: list[str], expected: list[str]) -> None:
        logger.debug("do_write_read")
        time.sleep(2)

        logger.debug("write data to stream")
        self.ydb_client.topic_write(self.input_topic, input_data)

        logger.debug("read data from stream")
        actual = self.ydb_client.topic_read_until(self.output_topic, self.consumer_name, len(expected))
        if len(actual) != len(expected):
            actual = actual[-len(expected):]  # deduplication disabled
        assert sorted(actual) == sorted(expected)

    def do_test_part1(self: Self) -> None:
        suffix = 'value1'
        input_data = [
            '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-1"}',
        ]
        expected = [
            '{"error_count":1,"host":"host-2","ts":"2025-01-01T00:00:00Z"}' + suffix,
            '{"error_count":2,"host":"host-1","ts":"2025-01-01T00:00:00Z"}' + suffix,
        ]
        self.do_write_read(input_data, expected)

    def do_test_part2(self: Self) -> None:
        suffix = 'value1'
        input_data = [
            '{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-2"}',
        ]
        expected = [
            '{"error_count":2,"host":"host-2","ts":"2025-01-01T00:10:00Z"}' + suffix,
            '{"error_count":1,"host":"host-1","ts":"2025-01-01T00:10:00Z"}' + suffix,
        ]
        self.do_write_read(input_data, expected)


class TestWatermarksMixedCluster(StreamingTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self: Self) -> Generator[None, None, None]:
        yield from self.setup_cluster()

    @link_test_case("#28606")
    @pytest.mark.parametrize("external", [True, False])
    def test_mixed_cluster(self: Self, external: bool) -> None:
        self.create_objects(external)
        self.create_streaming_query()
        self.do_test_part1()
        self.do_test_part2()


class TestWatermarksRestartToAnotherVersion(StreamingTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self: Self) -> Generator[None, None, None]:
        yield from self.setup_cluster()

    @link_test_case("#28606")
    @pytest.mark.parametrize("external", [True, False])
    def test_restart_to_another_version(self: Self, external: bool) -> None:
        self.create_objects(external)
        self.create_streaming_query()
        self.do_test_part1()
        self.change_cluster_version()
        self.do_test_part2()


class TestWatermarksRollingUpgradeAndDowngrade(StreamingTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self: Self) -> Generator[None, None, None]:
        yield from self.setup_cluster()

    @link_test_case("#28606")
    @pytest.mark.parametrize("external", [True, False])
    def test_rolling_upgrade(self: Self, external: bool) -> None:
        self.create_objects(external)
        self.create_simple_streaming_query()
        suffix = 'value1'

        for i, _ in enumerate(self.roll()):  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct during rolling upgrade
            #
            input_data = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}{suffix}']
            self.do_write_read(input_data, expected_data)
            time.sleep(0.5)
