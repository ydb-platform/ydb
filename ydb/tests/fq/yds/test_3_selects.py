#!/usr/bin/env python
# -*- coding: utf-8 -*-


import ydb.public.api.protos.ydb_value_pb2 as ydb


class TestSelects(object):
    def test_3_selects(self, ydb_cluster):
        results = ydb_cluster.ydb_client.query("""
            SELECT 1 AS SingleColumn;
            SELECT 'A' AS TextColumn;
            SELECT 11 AS Column1, 22 AS Column2;
        """)
        assert len(results) == 3
        rs = results[0]
        assert len(rs.rows) == 1
        assert len(rs.columns) == 1
        assert rs.columns[0].name == 'SingleColumn'
        assert rs.columns[0].type.type_id == ydb.Type.INT32
        assert rs.rows[0]['SingleColumn'] == 1

        rs = results[1]
        assert len(rs.rows) == 1
        assert len(rs.columns) == 1
        assert rs.columns[0].name == 'TextColumn'
        assert rs.columns[0].type.type_id == ydb.Type.STRING
        assert rs.rows[0]['TextColumn'] == b'A'

        rs = results[2]
        assert len(rs.rows) == 1
        assert len(rs.columns) == 2
        assert rs.columns[0].name == 'Column1'
        assert rs.columns[0].type.type_id == ydb.Type.INT32
        assert rs.rows[0]['Column1'] == 11
        assert rs.columns[1].name == 'Column2'
        assert rs.columns[1].type.type_id == ydb.Type.INT32
        assert rs.rows[0]['Column2'] == 22
