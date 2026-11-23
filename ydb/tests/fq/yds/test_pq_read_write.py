#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase


class TestPqReadWrite(TestYdsBase):
    @pytest.mark.parametrize("with_checkpoints", [True, False], ids=["with_checkpoints", "without_checkpoints"])
    def test_pq_read_write(self, ydb_topics, with_checkpoints):
        io = ydb_topics(f"pq_test_pq_read_write_{with_checkpoints}")
        consumer = io.name + "_input_consumer"
        io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')
        sql = Rf'''
            PRAGMA ydb.MaxTasksPerStage="2";
            PRAGMA ydb.DisableCheckpoints="{not with_checkpoints}";
            PRAGMA pq.Consumer="{consumer}";

            INSERT INTO {io.output}
            SELECT
                Yson::SerializeText(Yson::From(TableRow()))
            FROM (
                SELECT
                    k,
                    Sum(v) as sum
                FROM (
                    SELECT
                        Yson::LookupUint64(ys, "time") as t,
                        Yson::LookupInt64(ys, "key") as k,
                        Yson::LookupInt64(ys, "val") as v
                    FROM (
                        SELECT
                            Yson::Parse(Data) AS ys
                        FROM {io.input}))
                GROUP BY
                    k,
                    HOP(DateTime::FromMilliseconds(CAST(Unwrap(t) as Uint32)), "PT0.005S", "PT0.01S", "PT0.01S"));'''

        query = io.start(sql, with_checkpoints=with_checkpoints)

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

        io.write(data)

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

        assert io.read(len(expected)) == expected

        io.stop(query)
        io.wait_read_session_closed(io.input_topic, consumer)

    @pytest.mark.parametrize('with_checkpoints', [True, False], ids=['with_checkpoints', 'without_checkpoints'])
    def test_pq_read_schema_metadata(self, ydb_topics, with_checkpoints):
        io = ydb_topics(f'pq_read_schema_meta_{with_checkpoints}')
        query = io.start(
            f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            PRAGMA ydb.DisableCheckpoints = '{not with_checkpoints}';
            INSERT INTO {io.output}
            SELECT Unwrap(Yson::SerializeJson(Yson::From(TableRow())))
            FROM (
                SELECT field1, field2, __ydb_offset AS field3
                FROM {io.input}
                WITH (FORMAT=json_each_row, SCHEMA=(field1 String, field2 Int32))
            );
        """,
            with_checkpoints=with_checkpoints,
        )
        io.write(['{"field1": "value1", "field2": 105}', '{"field1": "value2", "field2": 106}'])
        expected = ['{"field1":"value1","field2":105,"field3":0}', '{"field1":"value2","field2":106,"field3":1}']
        assert io.read(len(expected)) == expected
        io.stop(query)
