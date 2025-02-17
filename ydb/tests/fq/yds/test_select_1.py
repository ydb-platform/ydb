#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1, yq_all

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from google.protobuf.struct_pb2 import NullValue


class TestSelect1(object):
    @yq_all
    def test_select_1(self, kikimr, client):
        sql = 'SELECT 1 AS SingleColumn;'
        query_id = client.create_query("simple1", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        describe_result = client.describe_query(query_id).result

        assert len(describe_result.query.plan.json) > 0, "plan must not be empty"
        assert len(describe_result.query.ast.data) > 0, "ast must not be empty"

        data = client.get_result_data(query_id)
        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "SingleColumn"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].int32_value == 1
        assert sum(kikimr.control_plane.get_metering(1)) == 10

    @yq_all
    def test_select_z_x_y(self, client):
        sql = "select 1 as z, 2 as x, 3 as y;"
        query_id = client.create_query("simple2", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)

        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 3
        assert result_set.columns[0].name == "z"
        assert result_set.columns[1].name == "x"
        assert result_set.columns[2].name == "y"
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].int32_value == 1
        assert result_set.rows[0].items[1].int32_value == 2
        assert result_set.rows[0].items[2].int32_value == 3

    @yq_v1
    def test_unwrap_null(self, client):
        sql = "select unwrap(1/0);"
        query_id = client.create_query("simple3", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_result = client.describe_query(query_id).result
        assert "Failed to unwrap empty optional" in describe_result.query.issue[0].issues[0].message

    @yq_all
    def test_select_pg(self, client):
        sql = R'''select ARRAY[ARRAY[1,2,3]], null, 'null', 1, true, null::int4'''

        query_id = client.create_query(
            "simple4", sql, type=fq.QueryContent.QueryType.ANALYTICS, pg_syntax=True
        ).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)

        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 6
        assert result_set.columns[0].name == "column0"
        assert result_set.columns[0].type.pg_type.oid == 1007
        assert result_set.columns[0].type.pg_type.typlen == -1
        assert result_set.columns[0].type.pg_type.typmod == -1
        assert result_set.columns[1].name == "column1"
        assert result_set.columns[1].type.null_type is not None
        assert result_set.columns[2].name == "column2"
        assert result_set.columns[2].type.pg_type.oid == 25
        assert result_set.columns[2].type.pg_type.typlen == -1
        assert result_set.columns[2].type.pg_type.typmod == -1
        assert result_set.columns[3].name == "column3"
        assert result_set.columns[3].type.pg_type.oid == 23
        assert result_set.columns[3].type.pg_type.typlen == 4
        assert result_set.columns[3].type.pg_type.typmod == -1
        assert result_set.columns[4].name == "column4"
        assert result_set.columns[4].type.pg_type.oid == 16
        assert result_set.columns[4].type.pg_type.typlen == 1
        assert result_set.columns[4].type.pg_type.typmod == -1
        assert result_set.columns[5].name == "column5"
        assert result_set.columns[5].type.pg_type.oid == 23
        assert result_set.columns[5].type.pg_type.typlen == 4
        assert result_set.columns[5].type.pg_type.typmod == -1

        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].text_value == "{{1,2,3}}"
        assert result_set.rows[0].items[1].null_flag_value == NullValue.NULL_VALUE
        assert result_set.rows[0].items[2].text_value == "null"
        assert result_set.rows[0].items[3].text_value == "1"
        assert result_set.rows[0].items[4].text_value == "t"
        assert result_set.rows[0].items[5].null_flag_value == NullValue.NULL_VALUE

    @yq_all
    def test_select_10_p_19_plus_1(self, client):
        sql = "SELECT 10000000000000000000+1 AS LargeColumn;"
        query_id = client.create_query("simple1", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        describe_string = str(client.describe_query(query_id).result)
        assert "Integral type implicit bitcast: Uint64 and Int32" in describe_string, describe_string

    @yq_all
    def test_compile_error(self, client, yq_version):
        sql = "SUPERSELECT;"
        query_id = client.create_query("simple1", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)
        describe_string = str(client.describe_query(query_id).result)
        assert (
            "Query failed with code " + ("ABORTED" if yq_version == "v1" else "GENERIC_ERROR") in describe_string
        ), describe_string
        assert "Unexpected token" in describe_string, describe_string
        # Failed to parse query is added in YQv1 only
        if yq_version == "v1":
            assert "Failed to parse query" in describe_string, describe_string

    @yq_all
    def test_ast_in_failed_query_runtime(self, client):
        sql = "SELECT unwrap(42 / 0) AS error_column"

        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.ANALYTICS).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.FAILED)

        ast = client.describe_query(query_id).result.query.ast.data
        assert "(\'\"error_column\" (Unwrap (/ (Int32 \'\"42\")" in ast, "Invalid query ast"
