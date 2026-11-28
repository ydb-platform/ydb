#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ydb
from ydb import convert
from ydb.public.api.protos import ydb_value_pb2

from ydb.tests.fq.mem_alloc.test_base import M, TIMEOUT


class TestResultLimits:
    def test_many_rows(self, kikimr):
        rows_count = 2_000_000
        payload = b'0123456789ABCDEF'
        # ExecuteQuery streams the full result instead of applying YQ's 20 MiB
        # stored-result limit. Consume each part without retaining all rows.
        sql = f'''
            SELECT * FROM AS_TABLE(
                () -> (Yql::ToStream(ListReplicate(<|x: "{payload.decode()}"|>, {rows_count})))
            );
        '''
        received = 0
        with kikimr.ydb_client.session_pool.checkout() as session:
            with session.execute(sql, settings=ydb.BaseRequestSettings().with_timeout(TIMEOUT)) as stream:
                for part in stream:
                    if part is None:
                        continue
                    assert not part.truncated
                    if part.columns:
                        assert len(part.columns) == 1
                        assert part.columns[0].name == 'x'
                        assert part.columns[0].type.type_id == ydb_value_pb2.Type.STRING
                    assert all(row['x'] == payload for row in part.rows)
                    received += len(part.rows)
        assert received == rows_count
        assert received * len(payload) > 20 * M

    def test_large_row(self, kikimr):
        items_count = 3_000_000
        sql = f'SELECT ListReplicate("A", {items_count}) AS large_row;'
        # The serialized Value holds more than 10 MiB, the old YQ row limit,
        # while staying below the Query Service response chunk limit.
        item_size = ydb_value_pb2.Value(items=[ydb_value_pb2.Value(bytes_value=b'A')]).ByteSize()
        assert items_count * item_size > 10 * M
        with kikimr.ydb_client.session_pool.checkout() as session:
            with session.execute(sql, settings=ydb.BaseRequestSettings().with_timeout(TIMEOUT)) as stream:
                result_sets = convert.aggregate_result_sets_by_index(stream)
        assert len(result_sets) == 1
        result = result_sets[0]
        assert not result.truncated
        assert len(result.columns) == 1
        assert result.columns[0].name == 'large_row'
        assert result.columns[0].type.list_type.item.type_id == ydb_value_pb2.Type.STRING
        assert len(result.rows) == 1
        value = result.rows[0]['large_row']
        assert len(value) == items_count
        assert all(item == b'A' for item in value)
