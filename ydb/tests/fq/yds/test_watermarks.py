#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.library.common.wait_for import wait_for

CONSUMER = 'watermarks_consumer'


def create_input_consumer(io):
    io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{CONSUMER}`;')


def check_completed_query(io, query, shared_reading):
    # Compilation warnings are persisted in Issues after successful completion;
    # ALTER RUN=false exposes only the cancellation issue instead.
    assert wait_for(
        lambda: io.state(query)['Status'] == 'COMPLETED',
        timeout_seconds=120,
        step_seconds=0.5,
    ), io.state(query)
    if shared_reading:
        issues = io.state(query)['Issues']
        assert "Row dispatcher will use the predicate:" in issues, issues
        assert "Row dispatcher will use watermark expr:" in issues, issues


class TestWatermarks(TestYdsBase):
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_watermarks(self, ydb_topics, shared_reading: bool):
        io = ydb_topics('test_watermarks', shared=shared_reading)
        create_input_consumer(io)

        ts = "CAST(ts AS Timestamp)" if shared_reading else '__ydb_write_time'

        sql = Rf'''
            PRAGMA pq.Consumer="{CONSUMER}";
            PRAGMA ydb.OptValidateStreamingCheckpoints="FALSE";
            -- Cannot guarantee writing to a specific partition
            PRAGMA ydb.MaxTasksPerStage="1";

            $input =
                SELECT
                    ts,
                    pass,
                    {ts} as hopTime
                FROM {io.input}
                WITH(
                    FORMAT=json_each_row,
                    SCHEMA(
                        ts String NOT NULL,
                        pass Uint64
                    )
                    , WATERMARK = {ts} - Interval("PT0.1S")
                    , WATERMARK_GRANULARITY = "PT0.5S"
                ) AS input;

            $output =
                SELECT
                    AGGREGATE_LIST(ts) AS result
                FROM $input
                WHERE pass > 0
                GROUP BY HoppingWindow(hopTime, "PT0.5S", "PT1S");

            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
            -- Complete after both expected windows to retain warnings in .sys/streaming_queries.
            FROM $output LIMIT 2;
        '''

        query_id = io.start(sql)

        io.write(
            [
                '{"ts": "1970-01-01T00:00:42Z", "pass": 1}',
                '{"ts": "1970-01-01T00:00:42Z", "pass": 0}',
            ]
        )
        assert io.read(1) == []

        expected = [
            '{"result":["1970-01-01T00:00:42Z"]}',
            '{"result":["1970-01-01T00:00:42Z"]}',
        ]
        read = []
        deadline = time.time() + 60
        while len(read) < len(expected):
            time.sleep(2)
            io.write(['{"ts": "1970-01-01T00:00:44Z", "pass": 0}'])
            read += io.read(len(expected) - len(read), timeout=5)
            assert time.time() < deadline, f"Expected {expected}, but got {read}"
        assert read == expected

        check_completed_query(io, query_id, shared_reading)

    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    def test_idle_watermarks(self, ydb_topics, shared_reading: bool, tasks: int):
        io = ydb_topics('test_idle_watermarks', partitions=2, shared=shared_reading)
        create_input_consumer(io)

        ts = "CAST(ts AS Timestamp)" if shared_reading else '__ydb_write_time'

        sql = Rf'''
            PRAGMA pq.Consumer="{CONSUMER}";
            PRAGMA ydb.OptValidateStreamingCheckpoints="FALSE";
            -- Cannot guarantee writing to a specific partition
            PRAGMA ydb.MaxTasksPerStage="{tasks}";

            $input =
                SELECT
                    ts,
                    pass,
                    {ts} as hopTime
                FROM {io.input}
                WITH(
                    FORMAT=json_each_row,
                    SCHEMA(
                        ts String NOT NULL,
                        pass Uint64
                    )
                    , WATERMARK = {ts} - Interval("PT0.1S")
                    , WATERMARK_IDLE_TIMEOUT = "PT0.2S"
                    , WATERMARK_GRANULARITY = "PT0.5S"
                ) AS input;

            $output =
                SELECT
                    AGGREGATE_LIST(ts) AS result
                FROM $input
                WHERE pass > 0
                GROUP BY HoppingWindow(hopTime, "PT0.5S", "PT1S");

            INSERT INTO {io.output}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
            -- Complete after both expected windows to retain warnings in .sys/streaming_queries.
            FROM $output LIMIT 2;
        '''

        query_id = io.start(sql)

        io.write(
            [
                '{"ts": "1970-01-01T00:00:42Z", "pass": 1}',
                '{"ts": "1970-01-01T00:00:42Z", "pass": 0}',
            ],
            partition_key=b'1',
        )
        assert io.read(1) == []

        time.sleep(2)

        io.write(['{"ts": "1970-01-01T00:00:44Z", "pass": 0}'], partition_key=b'1')
        expected = [
            '{"result":["1970-01-01T00:00:42Z"]}',
            '{"result":["1970-01-01T00:00:42Z"]}',
        ]
        assert io.read(len(expected)) == expected

        check_completed_query(io, query_id, shared_reading)
