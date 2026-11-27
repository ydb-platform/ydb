#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import ydb as ydb_sdk
import ydb.public.api.protos.ydb_value_pb2 as ydb_value

from ydb.tests.fq.streaming_common.common import StreamingTestBase


class TestBindings(StreamingTestBase):
    def test_yds_insert(self, ydb_topics):
        io = ydb_topics('yds_insert')
        client = io.kikimr.ydb_client
        consumer = io.name + '_input_consumer'
        io.kikimr.external_ydb_client.query(f'ALTER TOPIC `{io.input_topic}` ADD CONSUMER `{consumer}`;')

        # Topic sinks accept one serialized payload field. Keep the typed fields
        # in JSON and declare their schema explicitly when reading them back.
        client.query(f"""
            INSERT INTO {io.input}
            SELECT Unwrap(Yson::SerializeJson(Yson::From(TableRow()))) AS Data
            FROM AS_TABLE([<|foo:123, bar:"xxx"u|>, <|foo:456, bar:"yyy"u|>]);
        """)
        result_sets = client.query(f"""
            PRAGMA pq.Consumer = '{consumer}';
            SELECT foo, bar FROM {io.input} WITH (
                STREAMING = 'FALSE',
                FORMAT = json_each_row,
                SCHEMA = (foo Int32 NOT NULL, bar Utf8 NOT NULL)
            ) LIMIT 2;
        """)

        assert len(result_sets) == 1
        result = result_sets[0]
        assert len(result.columns) == 2
        assert result.columns[0].name == 'foo'
        assert result.columns[0].type.type_id == ydb_value.Type.INT32
        assert result.columns[1].name == 'bar'
        assert result.columns[1].type.type_id == ydb_value.Type.UTF8
        assert len(result.rows) == 2
        assert [(row['foo'], row['bar']) for row in result.rows] == [(123, 'xxx'), (456, 'yyy')]

    def test_raw_empty_schema_binding(self, ydb_topics):
        io = ydb_topics('pq_test_raw_empty_schema_binding')
        with pytest.raises(ydb_sdk.issues.Error) as error:
            io.kikimr.ydb_client.query(f"""
                SELECT * FROM {io.input} WITH SCHEMA() LIMIT 1;
            """)
        assert 'Only one column in schema supported in raw format' in str(error.value), str(error.value)
