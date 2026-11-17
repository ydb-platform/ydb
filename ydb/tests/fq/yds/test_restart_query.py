#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.fq.streaming_common.common import StreamingTestBase
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.tools.datastreams_helpers.control_plane import delete_stream


class TestRestartQuery(StreamingTestBase):
    @pytest.mark.parametrize('with_checkpoints', [False, True], ids=['without_checkpoints', 'with_checkpoints'])
    def test_restart_runtime_errors(self, ydb_topics, with_checkpoints):
        io = ydb_topics(f'restart_runtime_errors_{with_checkpoints}')
        query = io.start(
            f"""
                PRAGMA ydb.DisableCheckpoints = '{not with_checkpoints}';
                INSERT INTO {io.output} SELECT Data FROM {io.input};
            """,
            with_checkpoints=with_checkpoints,
        )

        def state():
            rows = io.kikimr.ydb_client.query(f"""
                SELECT Run, Status, Issues, RetryCount
                FROM `.sys/streaming_queries` WHERE Path = '{query}';
            """)[0].rows
            assert len(rows) == 1, rows
            return rows[0]

        io.write(['data'])
        assert io.read(1) == ['data']
        before = state()
        delete_stream(io.output_topic, default_endpoint=io.endpoint)
        # Force a sink operation even when no checkpoint flush is scheduled.
        io.write(['trigger_error'])
        last_state = before

        def has_runtime_error_and_retry():
            nonlocal last_state
            last_state = state()
            issues = last_state['Issues']
            return (
                last_state['RetryCount'] > before['RetryCount']
                and 'SCHEME_ERROR' in issues
                and 'will be restarted' in issues
            )

        assert wait_for(
            has_runtime_error_and_retry,
            timeout_seconds=plain_or_under_sanitizer(120, 500),
            step_seconds=0.5,
        ), last_state
        assert last_state['Run'], last_state
        # YDB exposes retry backoff as SUSPENDED while keeping Run enabled.
        assert last_state['Status'] in ('RUNNING', 'SUSPENDED'), last_state
        io.stop(query)
