#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for


class TestSelectTimings(StreamingTestBase):
    @pytest.mark.parametrize('success', [True, False], ids=['finished', 'aborted'])
    @pytest.mark.parametrize('with_restart', [False, True], ids=['without_restart', 'with_restart'])
    def test_select_timings(self, ydb_topics, success, with_restart):
        io = ydb_topics(f'select_timings_{success}_{with_restart}')
        query = io.start(f"""
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 1;
        """)
        timeout = plain_or_under_sanitizer(120, 300)

        def timings():
            rows = io.kikimr.ydb_client.query(f"""
                SELECT Status, CreatedAt, SubmittedAt, StartedAt, FinishedAt, RetryCount
                FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows
            assert len(rows) == 1, rows
            return rows[0]

        meta = timings()
        assert meta['Status'] == 'RUNNING', meta
        assert meta['CreatedAt'] is not None, meta
        assert meta['StartedAt'] is not None, meta
        assert meta['SubmittedAt'] == meta['StartedAt'], meta
        assert meta['CreatedAt'] <= meta['StartedAt'], meta
        assert meta['FinishedAt'] is None, meta
        created_at = meta['CreatedAt']
        started_at = meta['StartedAt']

        if with_restart:
            retries = meta['RetryCount']
            io.restart_compute_node(timeout=timeout)
            io.checkpoint(query, count=3, timeout=timeout)
            meta = timings()
            assert meta['Status'] == 'RUNNING', meta
            assert meta['RetryCount'] > retries, meta
            assert meta['CreatedAt'] == created_at, meta
            # StartedAt and SubmittedAt refer to the current script execution;
            # its start_ts is reset on retry. Resource CreatedAt remains stable.
            assert meta['StartedAt'] >= started_at, meta
            assert meta['SubmittedAt'] == meta['StartedAt'], meta
            assert meta['FinishedAt'] is None, meta
            started_at = meta['StartedAt']

        if success:
            io.write(['Message'])
            assert io.read(1) == ['Message']
            expected_status = 'COMPLETED'
        else:
            io.stop(query)
            expected_status = 'STOPPED'

        last_meta = meta

        def finished():
            nonlocal last_meta
            last_meta = timings()
            return last_meta['Status'] == expected_status and last_meta['FinishedAt'] is not None

        assert wait_for(finished, timeout_seconds=timeout, step_seconds=0.5), last_meta
        assert last_meta['CreatedAt'] == created_at, last_meta
        assert last_meta['StartedAt'] == started_at, last_meta
        assert last_meta['SubmittedAt'] == started_at, last_meta
        assert last_meta['FinishedAt'] >= started_at, last_meta
