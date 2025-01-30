#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest
import time

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase
from ydb.tests.tools.fq_runner.fq_client import StreamingDisposition

import ydb.public.api.protos.draft.fq_pb2 as fq


def start_yds_query(kikimr, client, sql, streaming_disposition) -> str:
    query_id = client.create_query(
        "simple", sql, streaming_disposition=streaming_disposition, type=fq.QueryContent.QueryType.STREAMING
    ).result.query_id
    client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
    kikimr.compute_plane.wait_zero_checkpoint(query_id)
    return query_id


def stop_yds_query(client, query_id):
    client.abort_query(query_id)
    client.wait_query(query_id)


class TestWatermarks(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_pq_watermarks(self, kikimr, client):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("pq_test_pq_watermarks")

        sql = Rf'''
            PRAGMA dq.WatermarksMode="default";
            PRAGMA dq.WatermarksGranularityMs="500";
            PRAGMA dq.WatermarksLateArrivalDelayMs="100";

            INSERT INTO yds.`{self.output_topic}`
            SELECT STREAM
                Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT AGGREGATE_LIST(field1) as data
                FROM (SELECT field1
                    FROM yds.`{self.input_topic}`
                    WITH (
                        format=json_each_row,
                        schema=(
                            field1 String,
                            field2 String
                        )
                    )
                    WHERE field2 == "abc1"
                )
                GROUP BY HoppingWindow("PT0.5S", "PT1S")
            );'''

        query_id = start_yds_query(kikimr, client, sql, streaming_disposition=StreamingDisposition.fresh())

        data1 = [
            '{"field1": "row1", "field2": "abc1"}',
            '{"field1": "row2", "field2": "abc2"}',
        ]
        self.write_stream(data1)

        time.sleep(2)  # wait for the write time in lb is moved forward

        data2 = [
            '{"field1": "row3", "field2": "abc3"}',
        ]
        self.write_stream(data2)

        expected = ['{"data" = ["row1"]}', '{"data" = ["row1"]}']

        assert self.read_stream(len(expected)) == expected

        stop_yds_query(client, query_id)

    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_idle_watermarks(self, kikimr, client):
        client.create_yds_connection(name="yds", database_id="FakeDatabaseId")
        self.init_topics("pq_test_idle_watermarks")

        sql = Rf'''
                PRAGMA dq.WatermarksMode="default";
                PRAGMA dq.WatermarksGranularityMs="500";
                PRAGMA dq.WatermarksLateArrivalDelayMs="2000";
                PRAGMA dq.WatermarksEnableIdlePartitions="true";

                INSERT INTO yds.`{self.output_topic}`
                SELECT STREAM
                    Yson::SerializeText(Yson::From(TableRow()))
                FROM (
                    SELECT AGGREGATE_LIST(field1) as data
                    FROM (SELECT field1
                        FROM yds.`{self.input_topic}`
                        WITH (
                            format=json_each_row,
                            schema=(
                                field1 String,
                                field2 String
                            )
                        )
                        WHERE field2 == "abc1"
                    )
                    GROUP BY HoppingWindow("PT0.5S", "PT1S")
                );'''

        query_id = start_yds_query(kikimr, client, sql, streaming_disposition=StreamingDisposition.fresh())

        data1 = [
            '{"field1": "row1", "field2": "abc1"}',
        ]
        self.write_stream(data1)

        expected = ['{"data" = ["row1"]}', '{"data" = ["row1"]}']

        assert self.read_stream(len(expected)) == expected

        stop_yds_query(client, query_id)
