#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

from ydb.tests.tools.datastreams_helpers.control_plane import list_read_rules

import ydb.public.api.protos.draft.fq_pb2 as fq

YDS_CONNECTION = "yds"


def start_yds_query(kikimr, client, sql, with_checkpoints) -> str:
    query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
    client.wait_query_status(query_id, fq.QueryMeta.RUNNING)
    if with_checkpoints:
        kikimr.compute_plane.wait_zero_checkpoint(query_id)
    else:
        kikimr.control_plane.wait_worker_count(1, "DQ_PQ_READ_ACTOR", 1)
    return query_id


def stop_yds_query(client, query_id):
    client.abort_query(query_id)
    client.wait_query(query_id)


class TestPqReadWrite(TestYdsBase):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    @pytest.mark.parametrize("with_checkpoints", [True, False], ids=["with_checkpoints", "without_checkpoints"])
    def test_pq_read_write(self, kikimr, client, with_checkpoints):
        client.create_yds_connection(name=YDS_CONNECTION, database_id="FakeDatabaseId")
        self.init_topics(Rf"pq_test_pq_read_write_{with_checkpoints}")
        sql = Rf'''
            PRAGMA dq.MaxTasksPerStage="2";
            PRAGMA dq.DisableCheckpoints="{not with_checkpoints}";

            INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
            SELECT STREAM
                Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT STREAM
                    k,
                    Sum(v) as sum
                FROM (
                    SELECT STREAM
                        Yson::LookupUint64(ys, "time") as t,
                        Yson::LookupInt64(ys, "key") as k,
                        Yson::LookupInt64(ys, "val") as v
                    FROM (
                        SELECT STREAM
                            Yson::Parse(Data) AS ys
                        FROM {YDS_CONNECTION}.`{self.input_topic}`))
                GROUP BY
                    k,
                    HOP(DateTime::FromMilliseconds(CAST(Unwrap(t) as Uint32)), "PT0.005S", "PT0.01S", "PT0.01S"));'''

        query_id = start_yds_query(kikimr, client, sql, with_checkpoints)

        # 100  105   110   115   120   125   130   135   140   (ms)
        #  [ Bucket1  )<--Delay1-->
        #       [  Bucket2  )<--Delay2-->
        #             [  Bucket3  )<--Delay3-->
        #                   [  Bucket4  )<--Delay4-->
        #                         [  Bucket5  )<--Delay5-->
        data = [
            '{"time" = 105; "key" = 1; "val" = 1;}',  # Group 1  Bucket 1, 2
            '{"time" = 107; "key" = 1; "val" = 4;}',  # Group 1  Bucket 1, 2
            '{"time" = 106; "key" = 2; "val" = 3;}',  # Group 2  Bucket 1, 2
            '{"time" = 111; "key" = 1; "val" = 7;}',  # Group 1  Bucket 2, 3
            '{"time" = 117; "key" = 1; "val" = 3;}',  # Group 1  Bucket 3, 4
            '{"time" = 110; "key" = 2; "val" = 2;}',  # Group 2  Bucket 2, 3
            '{"time" = 108; "key" = 1; "val" = 9;}',  # Group 1  Bucket 1, 2 (delayed)
            '{"time" = 121; "key" = 1; "val" = 4;}',  # Group 1  Bucket 4, 5 (close bucket 1)
            '{"time" = 107; "key" = 2; "val" = 2;}',  # Group 2  Bucket 1, 2 (delayed)
            '{"time" = 141; "key" = 2; "val" = 5;}',  # Group 2  Close all buckets
            '{"time" = 141; "key" = 1; "val" = 10;}',  # Group 1  Close all buckets
        ]

        self.write_stream(data)

        expected = [
            '{"k" = 1; "sum" = 14}',  # Group 1  Bucket 1
            '{"k" = 2; "sum" = 3}',  # Group 2  Bucket 1
            '{"k" = 2; "sum" = 7}',  # Group 2  Bucket 2
            '{"k" = 2; "sum" = 2}',  # Group 2  Bucket 3
            '{"k" = 1; "sum" = 21}',  # Group 1  Bucket 2
            '{"k" = 1; "sum" = 10}',  # Group 1  Bucket 3
            '{"k" = 1; "sum" = 7}',  # Group 1  Bucket 4
            '{"k" = 1; "sum" = 4}',  # Group 1  Bucket 5
        ]

        assert self.read_stream(len(expected)) == expected

        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 1, read_rules

        stop_yds_query(client, query_id)

        # Assert that all read rules were removed after query stops
        read_rules = list_read_rules(self.input_topic)
        assert len(read_rules) == 0, read_rules

    @yq_v1
    @pytest.mark.parametrize("with_checkpoints", [True, False], ids=["with_checkpoints", "without_checkpoints"])
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_pq_read_schema_metadata(self, kikimr, client, with_checkpoints):
        client.create_yds_connection(name=YDS_CONNECTION, database_id="FakeDatabaseId")
        self.init_topics(Rf"pq_read_schema_meta_{with_checkpoints}")
        sql = Rf'''
                PRAGMA dq.MaxTasksPerStage="2";
                PRAGMA dq.DisableCheckpoints="{not with_checkpoints}";

                INSERT INTO {YDS_CONNECTION}.`{self.output_topic}`
                SELECT UNWRAP(Yson::SerializeJson(Yson::From(TableRow())))
                FROM (
                    SELECT field1, field2, SystemMetadata("offset") as field3
                    FROM {YDS_CONNECTION}.`{self.input_topic}`
                    WITH (
                        format=json_each_row,
                        SCHEMA (
                            field1 String,
                            field2 Int32
                        )
                    )
                )'''

        query_id = start_yds_query(kikimr, client, sql, with_checkpoints)

        data1 = [
            '{"field1": "value1", "field2": 105}',
            '{"field1": "value2", "field2": 106}',
        ]
        self.write_stream(data1)

        expected = ['{"field1":"value1","field2":105,"field3":0}', '{"field1":"value2","field2":106,"field3":1}']

        assert self.read_stream(len(expected)) == expected

        stop_yds_query(client, query_id)
