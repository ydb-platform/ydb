#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1
from ydb.tests.tools.datastreams_helpers.test_yds_base import TestYdsBase

import ydb.public.api.protos.ydb_value_pb2 as ydb
import ydb.public.api.protos.draft.fq_pb2 as fq


class TestBindings(TestYdsBase):
    @yq_v1
    @pytest.mark.skip(reason="Is not implemented in YDS yet")
    def test_yds_insert(self, client):
        self.init_topics("yds_insert")

        connection_id = client.create_yds_connection(
            "ydsconnection", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT")
        ).result.connection_id

        foo_type = ydb.Column(name="foo", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.INT32))
        bar_type = ydb.Column(name="bar", type=ydb.Type(type_id=ydb.Type.PrimitiveTypeId.UTF8))
        client.create_yds_binding(
            name="ydsbinding",
            stream=self.input_topic,
            format="json_each_row",
            connection_id=connection_id,
            columns=[foo_type, bar_type],
        )

        sql = R'''
            insert into bindings.`ydsbinding`
            select * from AS_TABLE([<|foo:123, bar:"xxx"u|>,<|foo:456, bar:"yyy"u|>]);
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        sql = R'''
            select foo, bar from bindings.`ydsbinding` limit 2;
            '''

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        assert len(result_set.columns) == 2
        assert result_set.columns[0].name == "foo"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert result_set.columns[1].name == "bar"
        assert result_set.columns[1].type.type_id == ydb.Type.UTF8
        assert len(result_set.rows) == 2
        assert result_set.rows[0].items[0].int32_value == 123
        assert result_set.rows[0].items[1].text_value == 'xxx'
        assert result_set.rows[1].items[0].int32_value == 456
        assert result_set.rows[1].items[1].text_value == 'yyy'

    @yq_v1
    def test_raw_empty_schema_binding(self, kikimr, client, yq_version):
        self.init_topics(f"pq_test_raw_empty_schema_binding_{yq_version}")
        connection_response = client.create_yds_connection(
            "myyds2", os.getenv("YDB_DATABASE"), os.getenv("YDB_ENDPOINT")
        )
        assert not connection_response.issues, str(connection_response.issues)
        binding_response = client.create_yds_binding(
            name="my_binding",
            stream=self.input_topic,
            format="raw",
            connection_id=connection_response.result.connection_id,
            columns=[],
            check_issues=False,
        )
        assert "Only one column in schema supported in raw format" in str(binding_response.issues), str(
            binding_response.issues
        )
