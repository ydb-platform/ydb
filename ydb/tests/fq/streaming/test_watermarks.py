import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase

logger = logging.getLogger(__name__)


class TestWatermarksInYdb(StreamingTestBase):
<<<<<<< HEAD
    @pytest.mark.parametrize("kikimr", [{"enable_watermarks": True}], indirect=["kikimr"])
=======
>>>>>>> 371628977a6 ([YQ-5332] Watermarks: DQ: idle partitions fix (#43152))
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    @pytest.mark.parametrize("local_topics", [True, False])
    @pytest.mark.parametrize("enable_watermarks_advanced", [True, False])
    def test_watermarks(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int, local_topics: bool, enable_watermarks_advanced: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_watermarks_{shared_reading}{tasks}{local_topics}{enable_watermarks_advanced}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

<<<<<<< HEAD
        ts = "CAST(ts AS Timestamp)" if shared_reading else "SystemMetadata('write_time')"
=======
>>>>>>> 371628977a6 ([YQ-5332] Watermarks: DQ: idle partitions fix (#43152))
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

<<<<<<< HEAD
                $hop = (
                    SELECT
                        CAST(HOP_END() AS String) AS event_time,
                        AGGREGATE_LIST(ts) AS ts
                    FROM $filter
                    GROUP BY HoppingWindow(CAST(event_time AS Timestamp), "PT1S", "PT1S")
                );

                $output = (
                    SELECT
                        CAST(HOP_END() AS String) AS event_time,
                        AGGREGATE_LIST(ts) AS ts
                    FROM $hop
                    GROUP BY HoppingWindow(CAST(event_time AS Timestamp), "PT1S", "PT1S")
                );

                INSERT INTO {cluster}{self.output_topic}
                SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
                FROM $output;
=======
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
>>>>>>> 371628977a6 ([YQ-5332] Watermarks: DQ: idle partitions fix (#43152))
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
