#!/usr/bin/env python
# -*- coding: utf-8 -*-


import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestEval(object):
    def test_eval_2_2(self, ydb_cluster):
        result_set = ydb_cluster.ydb_client.query('SELECT EvaluateExpr(2 + 2) AS C1;')[0]
        assert len(result_set.columns) == 1
        assert result_set.columns[0].name == 'C1'
        assert result_set.columns[0].type.type_id == ydb.Type.INT32
        assert len(result_set.rows) == 1
        assert result_set.rows[0]['C1'] == 4
