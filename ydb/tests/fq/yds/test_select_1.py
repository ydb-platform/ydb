#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ydb.public.api.protos.ydb_value_pb2 as ydb

import pytest
import ydb as ydb_sdk
from ydb.convert import aggregate_result_sets_by_index
from ydb.tests.library.common.wait_for import wait_for


class TestSelect1(object):
    def test_select_1(self, ydb_cluster):
        with ydb_cluster.ydb_client.session_pool.checkout() as session:
            with session.execute('SELECT 1 AS SingleColumn;', stats_mode=ydb_sdk.QueryStatsMode.FULL) as stream:
                result_sets = aggregate_result_sets_by_index(stream)
            stats = session.last_query_stats

        assert stats is not None
        assert stats.query_plan, 'plan must not be empty'
        assert stats.query_ast, 'ast must not be empty'
        assert len(result_sets) == 1
        result = result_sets[0]
        assert len(result.columns) == 1
        assert result.columns[0].name == 'SingleColumn'
        assert result.columns[0].type.type_id == ydb.Type.INT32
        assert len(result.rows) == 1
        assert result.rows[0]['SingleColumn'] == 1

    def test_select_z_x_y(self, ydb_cluster):
        result_set = ydb_cluster.ydb_client.query('SELECT 1 AS z, 2 AS x, 3 AS y;')[0]
        assert len(result_set.columns) == 3
        assert [column.name for column in result_set.columns] == ['z', 'x', 'y']
        assert len(result_set.rows) == 1
        assert tuple(result_set.rows[0][name] for name in ('z', 'x', 'y')) == (1, 2, 3)

    def test_unwrap_null(self, ydb_cluster):
        with pytest.raises(ydb_sdk.issues.Error) as error:
            ydb_cluster.ydb_client.query('SELECT Unwrap(1 / 0);')
        assert 'Failed to unwrap empty optional' in str(error.value)

    def test_select_10_p_19_plus_1(self, ydb_topics):
        io = ydb_topics('select_10_p_19_plus_1')
        query = io.start(f"""
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output}
            SELECT CAST(10000000000000000000+-1 AS String) AS LargeColumn
            FROM {io.input} LIMIT 1;
        """)
        io.write(['trigger'])
        assert len(io.read(1)) == 1
        assert wait_for(
            lambda: io.state(query)['Status'] == 'COMPLETED',
            timeout_seconds=120,
            step_seconds=0.5,
        ), io.state(query)
        issues = io.state(query)['Issues']
        assert 'Integral type implicit bitcast: Uint64 and Int32' in issues, issues

    def test_compile_error(self, ydb_cluster):
        with pytest.raises(ydb_sdk.issues.GenericError) as error:
            ydb_cluster.ydb_client.query('SUPERSELECT;')
        issues = str(error.value)
        assert any(text in issues for text in ('Unexpected token', 'extraneous input', 'no viable alternative')), issues

    def test_ast_in_failed_query_runtime(self, ydb_topics):
        io = ydb_topics('ast_in_failed_query_runtime')
        query = io.start(f"""
            INSERT INTO {io.output}
            SELECT CAST(Unwrap(42 / CAST(Data AS Int32)) AS String) AS error_column FROM {io.input};
        """)
        # Fail at execution, after compilation and checkpoint creation have succeeded.
        io.write(['0'])
        last_state = None

        def runtime_failed_with_ast():
            nonlocal last_state
            rows = io.kikimr.ydb_client.query(f"""
                SELECT Status, Issues, Ast, Plan, RetryCount
                FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows
            assert len(rows) == 1, rows
            last_state = rows[0]
            return 'Failed to unwrap empty optional' in last_state['Issues'] and bool(last_state['Ast'])

        assert wait_for(runtime_failed_with_ast, timeout_seconds=120, step_seconds=0.5), last_state
        assert last_state['Plan'], last_state
        ast = last_state['Ast']
        assert 'error_column' in ast and 'Unwrap' in ast and '42' in ast, ast
        io.stop(query)
