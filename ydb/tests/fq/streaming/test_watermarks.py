import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase

logger = logging.getLogger(__name__)


class TestWatermarksInYdb(StreamingTestBase):
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_watermarks(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, local_topics: bool) -> None:
        if local_topics and shared_reading:
            pytest.skip("Shared reading is not supported for local topics: YQ-5036")

        tasks = 2
        endpoint = self.get_endpoint(kikimr, local_topics)
        query_name = f"test_watermarks_{shared_reading}{local_topics}"
        source_name = entity_name(query_name)
        self.init_topics(source_name, partitions_count=tasks, endpoint=endpoint)
        self.create_source(kikimr, source_name, shared_reading)

        cluster = f"{source_name}." if not local_topics else ""
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
            SELECT
                ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
            FROM
                $output
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
        time.sleep(30)  # leave a bit more time to fire up idle timeout

        expected = [
            '{"event_time":"1970-01-01T00:00:42Z","ts":[["1970-01-01T00:00:40Z"]]}',
            '{"event_time":"1970-01-01T00:00:52Z","ts":[["1970-01-01T00:00:50Z"]]}',
        ]
        actual = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        assert sorted(actual) == expected

        sql = f'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql)
