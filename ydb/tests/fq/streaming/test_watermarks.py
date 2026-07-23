import datetime
import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase

logger = logging.getLogger(__name__)


class TestWatermarksInYdb(StreamingTestBase):
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_watermarks(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_watermarks_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            $input = (
                SELECT
                    input.*,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    {cluster}{self.input_topic} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $hop = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM
                    $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            $output = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM
                    $hop
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        self.write_stream(
            data=[
                '{"ts": "1970-01-01T00:00:40Z", "pass": 1}',
                '{"ts": "1970-01-01T00:00:50Z", "pass": 1}',
                '{"ts": "1970-01-01T00:01:00Z", "pass": 0}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        expected = [
            '[["1970-01-01T00:00:40Z"]]',
            '[["1970-01-01T00:00:50Z"]]',
        ]
        actual = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        assert sorted(actual) == expected

        sql = f'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_wm_after_parsing(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool) -> None:
        if shared_reading:
            pytest.skip("Shared reading is not supported for watermarks after parsing yet")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_wm_after_parsing_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            $input = (
                SELECT
                    Yson::ConvertTo(Yson::ParseJson(line), Struct<ts: String, pass: Uint64>) AS row
                FROM
                    {cluster}{self.input_topic}
                    FLATTEN LIST BY (
                        String::SplitToList(Data, '.') AS line
                    )
                WHERE
                    line != ''
            );

            $input = (
                SELECT
                    ts,
                    pass
                FROM
                    $input
                    FLATTEN COLUMNS
            );

            $input = (
                SELECT
                    ts,
                    pass,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    $input WITH (
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $output = (
                SELECT
                    AGGREGATE_LIST(ts) AS ts
                FROM
                    $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        self.write_stream(
            data=[
                '{"ts": "1970-01-01T00:00:40Z", "pass": 1}..{"ts": "1970-01-01T00:00:50Z", "pass": 1}..{"ts": "1970-01-01T00:01:00Z", "pass": 0}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        expected = [
            '["1970-01-01T00:00:40Z"]',
            '["1970-01-01T00:00:50Z"]',
        ]
        actual = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        assert sorted(actual) == expected

        sql = f'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_early_events_are_dropped(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"wm_early_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            $input = (
                SELECT
                    input.*,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    {cluster}{self.input_topic} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $output = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)

        def time_format(time: datetime.datetime, delta: int) -> str:
            return (time + datetime.timedelta(minutes=delta)).isoformat().replace("+00:00", "Z")

        self.write_stream(
            data=[
                f'{{"ts": "{time_format(now,  0)}", "pass": 1}}',
                f'{{"ts": "{time_format(now, 10)}", "pass": 1}}',
                f'{{"ts": "{time_format(now,  4)}", "pass": 0}}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        actual = self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint)
        assert actual == [f'["{time_format(now, 0)}"]']

        kikimr.ydb_client.query(f'''DROP STREAMING QUERY `{query_name}`;''')

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("policy,expected", [
        ("DROP", ['["1970-01-01T00:00:50Z"]']),
        ("ADJUST", ['["1970-01-01T00:00:50Z"]', '["1970-01-01T00:00:40Z"]']),
    ])
    def test_late_events_policy(
        self: StreamingTestBase,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
        shared_reading: bool,
        tasks: int,
        local_topics: bool,
        policy: str,
        expected: list[str],
    ) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"wm_late_{policy.lower()}_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''
        sql = f'''
            CREATE STREAMING QUERY `{query_name}` WITH (
                WATERMARK_LATE_EVENTS_POLICY = {policy}
            ) AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';

            $input = (
                SELECT
                    input.*,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    {cluster}{self.input_topic} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $output = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        self.write_stream(
            data=[
                '{"ts": "1970-01-01T00:00:50Z", "pass": 1}',
                '{"ts": "1970-01-01T00:01:00Z", "pass": 0}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        actual = self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint)

        self.write_stream(
            data=[
                '{"ts": "1970-01-01T00:00:40Z", "pass": 1}',
                '{"ts": "1970-01-01T00:01:10Z", "pass": 0}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        if len(expected) > len(actual):
            actual += self.read_stream(len(expected) - len(actual), topic_path=self.output_topic, endpoint=endpoint)

        assert sorted(actual) == sorted(expected)

        kikimr.ydb_client.query(f'''DROP STREAMING QUERY `{query_name}`;''')

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("kikimr", [{"kqp_constraints_transformer": False}], indirect=["kikimr"])
    def test_watermarks_kqp_slj(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_watermarks_kqp_slj_{shared_reading}{tasks}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
        idleness_clause = ', WATERMARK_IDLE_TIMEOUT = "PT5S"' if tasks > 1 else ''

        table_name = entity_name("slj_table")
        kikimr.ydb_client.query(f"""
            CREATE TABLE `{table_name}` (
                key Int32 NOT NULL,
                value String,
                PRIMARY KEY (key)
            );
        """)
        kikimr.ydb_client.query(f"""
            UPSERT INTO `{table_name}` (key, value) VALUES (1, "one");
            UPSERT INTO `{table_name}` (key, value) VALUES (2, "two");
            UPSERT INTO `{table_name}` (key, value) VALUES (3, "three");
        """)

        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
            PRAGMA ydb.MaxTasksPerStage = '{tasks}';
            PRAGMA ydb.OverridePlanner = @@[
                {{"tx": 0, "stage": 0, "tasks": {tasks} }},
                {{"tx": 0, "stage": 1, "tasks": {tasks} }}
            ]@@;
            PRAGMA ydb.OptValidateStreamingConstraints="false";
            PRAGMA ydb.OptimizerHints = @@ JoinType(i db Lookup) @@;

            $input = (
                SELECT
                    input.*,
                    CAST(ts AS Timestamp) AS event_time,
                FROM
                    {cluster}{self.input_topic} WITH (
                        FORMAT = json_each_row,
                        SCHEMA (ts String, pass Uint64, k Int32),
                        WATERMARK = CAST(ts AS Timestamp) - Interval('PT5S')
                        {idleness_clause}
                    ) AS input
            );

            $input =
                SELECT
                    i.*,
                    db.*
                FROM $input AS i
                LEFT JOIN `{table_name}` AS db
                  ON i.k=db.key
                ;

            $hop = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(AsTuple(ts, value)) AS ts
                FROM
                    $input
                WHERE
                    pass > 0
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            $output = (
                SELECT
                    CAST(HOP_END() AS String) AS event_time,
                    AGGREGATE_LIST(ts) AS ts
                FROM
                    $hop
                GROUP BY
                    HoppingWindow(CAST(event_time AS Timestamp), 'PT1S', 'PT1S')
            );

            INSERT INTO {cluster}{self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
            FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        result_sets = kikimr.ydb_client.query(f"""
            SELECT Ast FROM `.sys/streaming_queries` WHERE Path = "/Root/{query_name}"
        """)
        assert "KqpCnStreamLookup" in result_sets[0].rows[0]["Ast"]

        self.write_stream(
            data=[
                '{"ts": "1970-01-01T00:00:40Z", "k":1, "pass": 1}',
                '{"ts": "1970-01-01T00:00:50Z", "k":2, "pass": 1}',
                '{"ts": "1970-01-01T00:01:00Z", "k":3, "pass": 0}',
            ],
            endpoint=endpoint,
            partition_key=b'1',
        )
        if shared_reading and tasks > 1:
            time.sleep(10)  # leave a bit more time to fire up idle timeout

        expected = [
            '[[["1970-01-01T00:00:40Z","one"]]]',
            '[[["1970-01-01T00:00:50Z","two"]]]',
        ]

        actual = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        assert sorted(actual) == expected

        sql = f'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql)
