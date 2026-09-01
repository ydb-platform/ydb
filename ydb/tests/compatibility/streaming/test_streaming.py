# -*- coding: utf-8 -*-
import logging
import os
import pytest
import time

from ydb.tests.fq.streaming_common.common import wait_completed_checkpoints
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture, RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.tools.datastreams_helpers.data_plane import write_stream, read_stream

logger = logging.getLogger(__name__)


class StreamingTestBase:
    def setup_cluster(self):
        logger.debug(f"setup_cluster, versions {self.versions}")

        if min(self.versions) < (25, 4):
            logger.debug("skip test, only available since 25-4")
            pytest.skip("Only available since 25-4")

        extra_feature_flags = [
            "enable_external_data_sources",
            "enable_streaming_queries",
            "enable_streaming_queries_counters"
        ]

        if min(self.versions) >= (26, 1) and min(self.versions) < (26, 2):
            # Feature was explicitly enabled in 26-1, and enabled by default in 26-2
            extra_feature_flags.append("enable_topics_sql_io_operations")

        os.environ["YDB_TEST_DEFAULT_CHECKPOINTING_PERIOD_MS"] = "200"
        os.environ["YDB_TEST_LEASE_DURATION_SEC"] = "15"
        yield from super().setup_cluster(
            disabled_feature_flags=["enable_drain_on_shutdown"],
            extra_feature_flags=extra_feature_flags,
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.TRACE,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.TRACE,
                'STREAMS_STORAGE_SERVICE': LogLevels.TRACE,
                'FQ_ROW_DISPATCHER': LogLevels.TRACE,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTER': LogLevels.DEBUG},
        )

    def create_objects(self, external: bool, with_precompute: bool = True):
        if not external and min(self.versions) < (26, 1):
            logger.debug("skip local topics, only available since 26-1")
            pytest.skip("Local topics only available since 26-1")

        logger.debug("create_objects")
        self.input_topic = 'streaming_recipe/input_topic'
        self.output_topic = 'streaming_recipe/output_topic'
        self.consumer_name = 'consumer_name'
        self.test_precompute_queries = with_precompute and min(self.versions) >= (26, 1)
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TOPIC `{self.input_topic}`;
                CREATE TOPIC `{self.output_topic}` (CONSUMER {self.consumer_name});
            """
            session_pool.execute_with_retries(query)

            if self.test_precompute_queries:
                create_query = """
                    CREATE TABLE table_name (
                        key Utf8,
                        value Utf8,
                        PRIMARY KEY (key)
                    );
                """
                session_pool.execute_with_retries(create_query)

                write_query = """
                    UPSERT INTO table_name (key, value) VALUES ('key1', 'value1');
                """
                session_pool.execute_with_retries(write_query)

        if external:
            self.create_external_data_source()
            self.input_object = f"`source_name`.`{self.input_topic}`"
            self.output_object = f"`source_name`.`{self.output_topic}`"
        else:
            self.input_object = f"`{self.input_topic}`"
            self.output_object = f"`{self.output_topic}`"

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

    def create_streaming_query(self):
        logger.debug("create_streaming_query")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN
                {'$precompute_data = SELECT value FROM table_name LIMIT 1;' if self.test_precompute_queries else ''}

                $input = (
                    SELECT * FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT host, COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HOP(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'),
                        host
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))){' || Unwrap($precompute_data)' if self.test_precompute_queries else ''}
                    FROM $number_errors
                );

                INSERT INTO {self.output_object}
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_simple_streaming_query(self):
        logger.debug("create_simple_streaming_query")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN
                {'$precompute_data = SELECT value FROM table_name LIMIT 1;' if self.test_precompute_queries else ''}

                $input = (
                    SELECT
                        *
                    FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))){' || Unwrap($precompute_data)' if self.test_precompute_queries else ''}
                    FROM $input
                );

                INSERT INTO {self.output_object}
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_join_objects(self):
        if min(self.versions) < (26, 1):
            logger.debug("skip local table join, only available since 26-1")
            pytest.skip("Streaming query with local table JOIN only available since 26-1")

        logger.debug("create_join_objects")
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries("""
                CREATE TABLE join_row_table (
                    id Utf8,
                    value Utf8,
                    PRIMARY KEY (id)
                );
            """)
            session_pool.execute_with_retries("""
                CREATE TABLE join_column_table (
                    id Utf8 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (id)
                ) WITH (
                    STORE = COLUMN
                );
            """)
            session_pool.execute_with_retries("""
                UPSERT INTO join_row_table (id, value) VALUES ('id1', '-row');
            """)
            session_pool.execute_with_retries("""
                UPSERT INTO join_column_table (id, value) VALUES ('id1', '-col');
            """)

    def create_streaming_query_with_join(self):
        logger.debug("create_streaming_query_with_join")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN

                $input = (
                    SELECT * FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT host, COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts, CAST("id1" AS Utf8) AS jk
                    FROM $filtered
                    GROUP BY
                        HOP(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'),
                        host
                );

                $joined = (
                    SELECT n.host AS host, n.error_count AS error_count, n.ts AS ts, r.value AS rv, c.value AS cv
                    FROM $number_errors AS n
                    LEFT JOIN join_row_table AS r ON n.jk = r.id
                    CROSS JOIN join_column_table AS c
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(AsStruct(host AS host, error_count AS error_count, ts AS ts))))) || Unwrap(rv) || Unwrap(cv)
                    FROM $joined
                );

                INSERT INTO {self.output_object}
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_simple_streaming_query_with_join(self):
        logger.debug("create_simple_streaming_query_with_join")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN

                $input = (
                    SELECT
                        i.*, CAST("id1" AS Utf8) AS jk
                    FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                    AS i
                );

                $joined = (
                    SELECT i.*, r.value AS rv, c.value AS cv
                    FROM $input AS i
                    LEFT JOIN join_row_table AS r ON i.jk = r.id
                    CROSS JOIN join_column_table AS c
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(AsStruct(host AS host, level AS level, time AS time))))) || Unwrap(rv) || Unwrap(cv)
                    FROM $joined
                );

                INSERT INTO {self.output_object}
                SELECT * FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_multi_output_objects(self, external):
        if min(self.versions) < (26, 1):
            logger.debug("skip multi output into YDB table, only available since 26-1")
            pytest.skip("Streaming query output into YDB table only available since 26-1")

        logger.debug("create_multi_output_objects")
        self.create_objects(external, with_precompute=False)
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries("""
                CREATE TABLE multi_output_table (
                    data String NOT NULL,
                    PRIMARY KEY (data)
                );
            """)

    def create_streaming_query_with_multi_output(self):
        logger.debug("create_streaming_query_with_multi_output")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN
                $input = (
                    SELECT * FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );
                $filtered = (SELECT * FROM $input WHERE level == 'error');

                $number_errors = (
                    SELECT host, COUNT(*) AS error_count, CAST(HOP_START() AS String) AS ts
                    FROM $filtered
                    GROUP BY
                        HOP(CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'),
                        host
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) AS data
                    FROM $number_errors
                );

                INSERT INTO {self.output_object}
                SELECT data FROM $json;

                UPSERT INTO multi_output_table
                SELECT data FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def create_simple_streaming_query_with_multi_output(self):
        logger.debug("create_simple_streaming_query_with_multi_output")
        self.query_name = "my_queries/query_name"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE STREAMING QUERY `{self.query_name}` AS DO BEGIN
                $input = (
                    SELECT
                        *
                    FROM
                        {self.input_object} WITH (
                            FORMAT = 'json_each_row',
                            SCHEMA (time String NOT NULL, level String NOT NULL, host String NOT NULL)
                        )
                );

                $json = (SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow())))) AS data
                    FROM $input
                );

                INSERT INTO {self.output_object}
                SELECT data FROM $json;

                UPSERT INTO multi_output_table
                SELECT data FROM $json;
                END DO;

            """
            session_pool.execute_with_retries(query)

    def check_multi_output_table(self, expected_count):
        logger.debug("check_multi_output_table")
        # The table sink may commit slightly after the topic output is read, so poll until consistent
        deadline = time.time() + 30
        count = None
        while True:
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result_sets = session_pool.execute_with_retries("SELECT COUNT(*) AS cnt FROM multi_output_table")
                count = result_sets[0].rows[0].cnt
            if count == expected_count:
                return
            assert time.time() < deadline, f"multi_output_table expected {expected_count} rows, got {count}"
            time.sleep(1)

    def do_write_read(self, input, expected_output):
        logger.debug("do_write_read")
        endpoint = f"localhost:{self.cluster.nodes[1].port}"
        time.sleep(2)
        logger.debug("write data to stream")
        write_stream(path=self.input_topic, data=input, database=self.database_path, endpoint=endpoint)
        logger.debug("read data from stream")
        read_data = read_stream(
            path=self.output_topic,
            messages_count=len(expected_output),
            consumer_name=self.consumer_name,
            database=self.database_path,
            endpoint=endpoint,
            timeout=plain_or_under_sanitizer(60, 300))
        if (len(read_data) != len(expected_output)):
            read_data = read_data[-len(expected_output):]        # deduplication disabled
        assert sorted(read_data) == sorted(expected_output)

    def do_test_part1(self, extra_suffix=''):
        suffix = ('value1' if self.test_precompute_queries else '') + extra_suffix
        input = [
            '{"time": "2025-01-01T00:00:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:04:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:08:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:12:00.000000Z", "level": "error", "host": "host-1"}']
        expected_data = sorted([
            '{"error_count":1,"host":"host-2","ts":"2025-01-01T00:00:00Z"}' + suffix,
            '{"error_count":2,"host":"host-1","ts":"2025-01-01T00:00:00Z"}' + suffix])
        self.do_write_read(input, expected_data)

    def do_test_part2(self, extra_suffix=''):
        suffix = ('value1' if self.test_precompute_queries else '') + extra_suffix
        input = [
            '{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-2"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-1"}',
            '{"time": "2025-01-01T00:22:00.000000Z", "level": "error", "host": "host-2"}']
        expected_data = sorted([
            '{"error_count":2,"host":"host-2","ts":"2025-01-01T00:10:00Z"}' + suffix,
            '{"error_count":1,"host":"host-1","ts":"2025-01-01T00:10:00Z"}' + suffix])
        self.do_write_read(input, expected_data)


class TestStreamingMixedCluster(StreamingTestBase, MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#27924")
    @pytest.mark.parametrize("external", [True, False])
    def test_mixed_cluster(self, external):
        self.create_objects(external)
        self.create_streaming_query()
        self.do_test_part1()
        self.do_test_part2()

    @link_test_case("#46772")
    @pytest.mark.parametrize("external", [True, False])
    def test_mixed_cluster_join(self, external):
        self.create_objects(external, with_precompute=False)
        self.create_join_objects()
        self.create_streaming_query_with_join()
        self.do_test_part1(extra_suffix='-row-col')
        self.do_test_part2(extra_suffix='-row-col')

    @link_test_case("#48465")
    @pytest.mark.parametrize("external", [True, False])
    def test_mixed_cluster_multi_output(self, external):
        self.create_multi_output_objects(external)
        self.create_streaming_query_with_multi_output()
        self.do_test_part1()
        self.check_multi_output_table(2)
        self.do_test_part2()
        self.check_multi_output_table(4)


class TestStreamingRestartToAnotherVersion(StreamingTestBase, RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#27924")
    @pytest.mark.parametrize("external", [True, False])
    def test_restart_to_another_version(self, external):
        self.create_objects(external)
        self.create_streaming_query()
        self.do_test_part1()
        wait_completed_checkpoints(self.cluster, f"/Root/{self.query_name}", checkpoints_count=1, wait_delta=False)
        self.change_cluster_version()
        self.do_test_part2()

    @link_test_case("#46772")
    @pytest.mark.parametrize("external", [True, False])
    def test_restart_to_another_version_join(self, external):
        self.create_objects(external, with_precompute=False)
        self.create_join_objects()
        self.create_streaming_query_with_join()
        self.do_test_part1(extra_suffix='-row-col')
        wait_completed_checkpoints(self.cluster, f"/Root/{self.query_name}", checkpoints_count=1, wait_delta=False)
        self.change_cluster_version()
        self.do_test_part2(extra_suffix='-row-col')

    @link_test_case("#48465")
    @pytest.mark.parametrize("external", [True, False])
    def test_restart_to_another_version_multi_output(self, external):
        self.create_multi_output_objects(external)
        self.create_streaming_query_with_multi_output()
        self.do_test_part1()
        self.check_multi_output_table(2)
        wait_completed_checkpoints(self.cluster, f"/Root/{self.query_name}", checkpoints_count=1, wait_delta=False)
        self.change_cluster_version()
        self.do_test_part2()
        self.check_multi_output_table(4)


class TestStreamingRollingUpgradeAndDowngrade(StreamingTestBase, RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @link_test_case("#27924")
    @pytest.mark.parametrize("external", [True, False])
    def test_rolling_upgrade(self, external):
        self.create_objects(external)
        self.create_simple_streaming_query()
        suffix = 'value1' if self.test_precompute_queries else ''

        for i, _ in enumerate(self.roll()):  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct during rolling upgrade
            #
            input = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}{suffix}']
            self.do_write_read(input, expected_data)
            time.sleep(0.5)

    @link_test_case("#46772")
    @pytest.mark.parametrize("external", [True, False])
    def test_rolling_upgrade_join(self, external):
        self.create_objects(external, with_precompute=False)
        self.create_join_objects()
        self.create_simple_streaming_query_with_join()
        suffix = '-row-col'

        for i, _ in enumerate(self.roll()):  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct during rolling upgrade
            #
            input = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}{suffix}']
            self.do_write_read(input, expected_data)
            time.sleep(0.5)

    @link_test_case("#48465")
    @pytest.mark.parametrize("external", [True, False])
    def test_rolling_upgrade_multi_output(self, external):
        self.create_multi_output_objects(external)
        self.create_simple_streaming_query_with_multi_output()

        for i, _ in enumerate(self.roll()):  # every iteration is a step in rolling upgrade process
            #
            # 2. check written data is correct in both outputs during rolling upgrade
            #
            input = [f'{{"time": "2025-01-01T00:15:00.000000Z", "level": "error", "host": "host-{i}"}}']
            expected_data = [f'{{"host":"host-{i}","level":"error","time":"2025-01-01T00:15:00.000000Z"}}']
            self.do_write_read(input, expected_data)
            self.check_multi_output_table(i + 1)
            time.sleep(0.5)
