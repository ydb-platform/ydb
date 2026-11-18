#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.fq.streaming_common.common import StreamingTestBase


class TestSelectLimit(StreamingTestBase):
    def test_select_limit(self, ydb_topics):
        io = ydb_topics('select_limit')
        future = io.start_select(
            f'SELECT Data FROM {io.input} WITH (STREAMING = "TRUE") LIMIT 2;',
            [io.input_topic],
        )
        messages = [b'A', b'B', b'C']
        io.write(messages)
        result_sets = future.result(timeout=120)

        assert len(result_sets) == 1
        result = result_sets[0]
        assert len(result.rows) == 2
        assert len(result.columns) == 1
        assert [row['Data'] for row in result.rows] == messages[:2]
        io.wait_read_session_closed(io.input_topic, io.name + '_select_consumer')
