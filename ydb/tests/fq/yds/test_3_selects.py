#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import pytest

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb
from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1


class TestSelects(object):
    @yq_v1
    @pytest.mark.parametrize("mvp_external_ydb_endpoint", [{"endpoint": os.getenv("YDB_ENDPOINT")}], indirect=True)
    def test_3_selects(self, client):
        sql = R'''
            pragma dq.Scheduler=@@{"type": "single_node"}@@;
            SELECT 1 AS SingleColumn;
            SELECT "A" AS TextColumn;
            SELECT 11 AS Column1, 22 AS Column2;
        '''

        client.create_yds_connection(name="myyds", database_id="FakeDatabaseId")
        query_id = client.create_query("simple", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id

        client.wait_query(query_id, 30)

        rs = client.get_result_data(query_id, result_set_index=0).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 1
        assert len(rs.columns) == 1
        assert rs.columns[0].name == "SingleColumn"
        assert rs.columns[0].type.type_id == ydb.Type.INT32
        assert rs.rows[0].items[0].int32_value == 1

        rs = client.get_result_data(query_id, result_set_index=1).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 1
        assert len(rs.columns) == 1
        assert rs.columns[0].name == "TextColumn"
        assert rs.columns[0].type.type_id == ydb.Type.STRING
        assert rs.rows[0].items[0].bytes_value == b"A"

        rs = client.get_result_data(query_id, result_set_index=2).result.result_set
        logging.debug(str(rs))
        assert len(rs.rows) == 1
        assert len(rs.columns) == 2
        assert rs.columns[0].name == "Column1"
        assert rs.columns[0].type.type_id == ydb.Type.INT32
        assert rs.rows[0].items[0].int32_value == 11
        assert rs.columns[1].name == "Column2"
        assert rs.columns[1].type.type_id == ydb.Type.INT32
        assert rs.rows[0].items[1].int32_value == 22
