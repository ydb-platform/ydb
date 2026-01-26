import logging
import pytest
import time
from typing import Callable

from ydb.tests.fq.streaming.common import Kikimr, StreamingTestBase

logger = logging.getLogger(__name__)


class TestWatermarksInYdb(StreamingTestBase):
    @pytest.mark.parametrize("kikimr", [{"enable_watermarks": True}], indirect=["kikimr"])
    @pytest.mark.parametrize("shared_reading", [False], ids=["no_shared"])
    @pytest.mark.parametrize("tasks", [1])
    def test_watermarks(self: StreamingTestBase, kikimr: Kikimr, entity_name: Callable[[str], str], shared_reading: bool, tasks: int) -> None:
        source_name = entity_name("test_watermarks")
        self.init_topics(source_name, partitions_count=tasks)
        self.create_source(kikimr, source_name, shared_reading)

        event_time = "ts" if shared_reading else "write_time"

        query_name = "test_watermarks"
        sql = f'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                USE {source_name};
                PRAGMA ydb.MaxTasksPerStage="{tasks}";

                $input = (
                    SELECT
                        {self.input_topic}.*,
                        SystemMetadata("write_time") as write_time
                    FROM {self.input_topic}
                    WITH (
                        FORMAT=json_each_row,
                        SCHEMA (
                            ts String NOT NULL,
                            pass Uint64
                        )
                    )
                );

                $filter = (
                    SELECT
                        input.*,
                        {event_time} AS event_time
                    FROM $input AS input
                    WHERE pass > 0
                );

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

                INSERT INTO {self.output_topic}
                SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(ts))))
                FROM $output;
            END DO;
        '''
        kikimr.ydb_client.query(sql)
        self.wait_completed_checkpoints(kikimr, f"/Root/{query_name}")

        self.write_stream(['{"ts": "1970-01-01T00:00:40Z", "pass": 1}'])
        time.sleep(10)
        self.write_stream(['{"ts": "1970-01-01T00:00:50Z", "pass": 1}'])
        time.sleep(10)
        self.write_stream(['{"ts": "1970-01-01T00:01:00Z", "pass": 0}'])

        expected = [
            '[["1970-01-01T00:00:40Z"]]',
            '[["1970-01-01T00:00:50Z"]]',
        ]
        actual = self.read_stream(len(expected), topic_path=self.output_topic)
        assert actual == expected

        sql = f'''DROP STREAMING QUERY `{query_name}`;'''
        kikimr.ydb_client.query(sql)
