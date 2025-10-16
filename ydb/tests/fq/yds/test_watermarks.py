#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import time

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.kikimr_runner import TenantConfig
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimr
from ydb.tests.tools.fq_runner.kikimr_runner import StreamingOverKikimrConfig
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient

import ydb.public.api.protos.draft.fq_pb2 as fq


YDS_CONNECTION = "yds"
COMPUTE_NODE_COUNT = 1


@pytest.fixture
def kikimr(request):
    kikimr_conf = StreamingOverKikimrConfig(
        cloud_mode=True, node_count={"/cp": TenantConfig(1), "/compute": TenantConfig(COMPUTE_NODE_COUNT)}
    )
    kikimr = StreamingOverKikimr(kikimr_conf)
    kikimr.compute_plane.fq_config['row_dispatcher']['enabled'] = True
    kikimr.compute_plane.fq_config['row_dispatcher']['without_consumer'] = True
    kikimr.start_mvp_mock_server()
    kikimr.start()
    yield kikimr
    kikimr.stop_mvp_mock_server()
    kikimr.stop()


def start_yds_query(kikimr: StreamingOverKikimr, client: FederatedQueryClient, sql: str) -> str:
    query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
    client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
    kikimr.compute_plane.wait_zero_checkpoint(query_id)
    return query_id


def stop_yds_query(client: FederatedQueryClient, query_id: str) -> None:
    client.abort_query(query_id)
    client.wait_query(query_id)


class TestWatermarks(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    def test_watermarks(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient, shared_reading: bool):
        client.create_yds_connection(
            name=YDS_CONNECTION, database=os.getenv("YDB_DATABASE"), endpoint=os.getenv("YDB_ENDPOINT"), shared_reading=shared_reading
        )
        self.init_topics(f"test_watermarks_{"shared" if shared_reading else "no_shared"}")

        ts = "ts" if shared_reading else "write_time"
        watermark_expr = ", WATERMARK AS (Unwrap(CAST(ts AS Timestamp) - Interval(\"PT0.1S\")))" if shared_reading else ""

        sql = Rf'''
            USE {YDS_CONNECTION};
            -- Cannot guarantee writing to a specific partition
            PRAGMA dq.MaxTasksPerStage="1";
            PRAGMA dq.WatermarksMode="default";
            PRAGMA dq.WatermarksGranularityMs="500";
            PRAGMA dq.WatermarksLateArrivalDelayMs="100";

            $input =
                SELECT
                    {self.input_topic}.*,
                    SystemMetadata("write_time") as write_time
                FROM {self.input_topic}
                WITH(
                    FORMAT=json_each_row,
                    SCHEMA(
                        ts String NOT NULL,
                        pass Uint64
                    )
                    {watermark_expr}
                );

            $output =
                SELECT
                    AGGREGATE_LIST(ts) AS result
                FROM $input
                WHERE pass > 0
                GROUP BY HoppingWindow(CAST({ts} AS Timestamp), "PT0.5S", "PT1S");

            INSERT INTO {self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
            FROM $output;
        '''

        query_id = start_yds_query(kikimr, client, sql)

        self.write_stream([
            '{"ts": "1970-01-01T00:00:42Z", "pass": 1}',
            '{"ts": "1970-01-01T00:00:42Z", "pass": 0}',
        ])
        assert self.read_stream(1) == []

        time.sleep(2)

        self.write_stream(['{"ts": "1970-01-01T00:00:44Z", "pass": 0}'])
        expected = [
            '{"result":["1970-01-01T00:00:42Z"]}',
            '{"result":["1970-01-01T00:00:42Z"]}',
        ]
        assert self.read_stream(len(expected)) == expected

        stop_yds_query(client, query_id)

        if shared_reading:
            issues = str(client.describe_query(query_id).result.query.transient_issue)
            assert "Row dispatcher will use the predicate:" in issues, issues
            issues = str(client.describe_query(query_id).result.query.transient_issue)
            assert "Row dispatcher will use watermark expr:" in issues, issues

    @yq_v1
    @pytest.mark.parametrize("shared_reading", [False, True], ids=["no_shared", "shared"])
    @pytest.mark.parametrize("tasks", [1, 2])
    def test_idle_watermarks(self, kikimr: StreamingOverKikimr, client: FederatedQueryClient, shared_reading: bool, tasks: int):
        client.create_yds_connection(
            name=YDS_CONNECTION, database=os.getenv("YDB_DATABASE"), endpoint=os.getenv("YDB_ENDPOINT"), shared_reading=shared_reading
        )
        self.init_topics(f"test_idle_watermarks_{"shared" if shared_reading else "no_shared"}", partitions_count=2)

        ts = "ts" if shared_reading else "write_time"
        watermark_expr = ", WATERMARK AS (Unwrap(CAST(ts AS Timestamp) - Interval(\"PT0.1S\")))" if shared_reading else ""

        sql = Rf'''
            USE {YDS_CONNECTION};
            -- Cannot guarantee writing to a specific partition
            PRAGMA dq.MaxTasksPerStage="{tasks}";
            PRAGMA dq.WatermarksMode="default";
            PRAGMA dq.WatermarksGranularityMs="500";
            PRAGMA dq.WatermarksLateArrivalDelayMs="100";
            PRAGMA dq.WatermarksIdleDelayMs="200";
            PRAGMA dq.WatermarksEnableIdlePartitions="true";

            $input =
                SELECT
                    {self.input_topic}.*,
                    SystemMetadata("write_time") as write_time
                FROM {self.input_topic}
                WITH(
                    FORMAT=json_each_row,
                    SCHEMA(
                        ts String NOT NULL,
                        pass Uint64
                    )
                    {watermark_expr}
                );

            $output =
                SELECT
                    AGGREGATE_LIST(ts) AS result
                FROM $input
                WHERE pass > 0
                GROUP BY HoppingWindow(CAST({ts} AS Timestamp), "PT0.5S", "PT1S");

            INSERT INTO {self.output_topic}
            SELECT ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
            FROM $output;
        '''

        query_id = start_yds_query(kikimr, client, sql)

        self.write_stream([
            '{"ts": "1970-01-01T00:00:42Z", "pass": 1}',
            '{"ts": "1970-01-01T00:00:42Z", "pass": 0}',
        ], partition_key=b'1')
        assert self.read_stream(1) == []

        time.sleep(2)

        self.write_stream(['{"ts": "1970-01-01T00:00:44Z", "pass": 0}'], partition_key=b'1')
        expected = [
            '{"result":["1970-01-01T00:00:42Z"]}',
            '{"result":["1970-01-01T00:00:42Z"]}',
        ]
        assert self.read_stream(len(expected)) == expected

        stop_yds_query(client, query_id)

        if shared_reading:
            issues = str(client.describe_query(query_id).result.query.transient_issue)
            assert "Row dispatcher will use the predicate:" in issues, issues
            issues = str(client.describe_query(query_id).result.query.transient_issue)
            assert "Row dispatcher will use watermark expr:" in issues, issues
