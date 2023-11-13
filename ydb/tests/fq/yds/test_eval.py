#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from ydb.tests.tools.fq_runner.kikimr_utils import yq_v1

import ydb.public.api.protos.draft.fq_pb2 as fq
import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestEval(object):
    @yq_v1
    def test_eval_2_2(self, client):
        sql = "SELECT EvaluateExpr(2+2) AS C1;"
        query_id = client.create_query("simple1", sql, type=fq.QueryContent.QueryType.STREAMING).result.query_id
        client.wait_query_status(query_id, fq.QueryMeta.COMPLETED)
        data = client.get_result_data(query_id)

        result_set = data.result.result_set
        logging.debug(str(result_set))
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == "C1"
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0].items[0].int32_value == 4
