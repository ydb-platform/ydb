#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from ydb.tests.library.common.wait_for import wait_for


def check_running_metrics(io, query, input_bytes):
    last_metrics = {}

    def available():
        nonlocal last_metrics
        last_metrics = io.metrics(query)
        expected = {
            'streaming.query.tasks.count',
            'streaming.query.cpu.usage.milliseconds',
            'streaming.query.memory.usage.bytes',
            'streaming.query.input.bytes',
            'streaming.query.output.bytes',
            'streaming.query.uptime.seconds',
        }
        return (
            expected <= last_metrics.keys()
            and last_metrics['streaming.query.tasks.count'] > 0
            and last_metrics['streaming.query.cpu.usage.milliseconds'] > 0
            and last_metrics['streaming.query.memory.usage.bytes'] > 0
            and last_metrics['streaming.query.input.bytes'] >= input_bytes
            and last_metrics['streaming.query.output.bytes'] >= input_bytes
            and last_metrics['streaming.query.uptime.seconds'] >= 0
        )

    assert wait_for(available, timeout_seconds=120, step_seconds=0.5), last_metrics


def check_finished_plan(io, query, input_bytes):
    rows = io.kikimr.ydb_client.query(f"""
        SELECT Plan, StartedAt, FinishedAt FROM `.sys/streaming_queries` WHERE Path = '{query}';
    """)[0].rows
    assert len(rows) == 1, rows
    plan = json.loads(rows[0]['Plan'])

    def stages(node):
        if isinstance(node, dict):
            if isinstance(node.get('Stats'), dict):
                yield node['Stats']
            for value in node.values():
                yield from stages(value)
        elif isinstance(node, list):
            for value in node:
                yield from stages(value)

    stats = list(stages(plan))
    for name, minimum in (
        ('CpuTimeUs', 1),
        ('MaxMemoryUsage', 1),
        ('IngressBytes', input_bytes),
        ('EgressBytes', input_bytes),
    ):
        values = [stage.get(name, {}).get('Sum', 0) for stage in stats]
        assert any(value >= minimum for value in values), (name, minimum, values, plan)
    assert rows[0]['StartedAt'] is not None and rows[0]['FinishedAt'] is not None, rows
    assert rows[0]['FinishedAt'] >= rows[0]['StartedAt'], rows


class TestPublicMetrics:
    def test_select_limit(self, ydb_topics):
        io = ydb_topics('public_select_limit')
        query = io.start(f"""
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 2;
        """)
        first = 'A' * (64 * 1024)
        io.write([first])
        assert io.read(1) == [first]
        # KQP removes the metric group on completion, so observe the values
        # while the limited query is still running, then verify their cleanup.
        check_running_metrics(io, query, len(first))
        io.write(['finish'])
        assert io.read(1) == ['finish']
        assert wait_for(
            lambda: io.state(query)['Status'] == 'COMPLETED', timeout_seconds=120, step_seconds=0.5
        ), io.state(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        check_finished_plan(io, query, len(first))
        assert wait_for(lambda: not io.metrics(query), timeout_seconds=120, step_seconds=0.5), io.metrics(query)

    def test_select_unlimited(self, ydb_topics):
        io = ydb_topics('public_select_unlimited')
        query = io.start(f'INSERT INTO {io.output} SELECT Data FROM {io.input};')
        data = [symbol * (64 * 1024) for symbol in ('A', 'B', 'C', 'D')]
        io.write(data)
        assert io.read(len(data)) == data
        check_running_metrics(io, query, sum(map(len, data)))
        io.stop(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        check_finished_plan(io, query, sum(map(len, data)))
        assert wait_for(lambda: not io.metrics(query), timeout_seconds=120, step_seconds=0.5), io.metrics(query)

    def test_ingress_bytes(self, ydb_topics):
        io = ydb_topics('ingress_bytes')
        query = io.start(f'INSERT INTO {io.output} SELECT Data FROM {io.input};')
        data = ['1' * (1024 * 1024)]
        for _ in range(15):
            io.write(data)
        assert io.read(15) == data * 15
        assert wait_for(
            lambda: io.metrics(query).get('streaming.query.input.bytes', 0) >= 15 * 1024 * 1024,
            timeout_seconds=120,
            step_seconds=0.5,
        ), io.metrics(query)
        io.stop(query)
        check_finished_plan(io, query, 15 * 1024 * 1024)
