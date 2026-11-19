#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from ydb.tests.fq.streaming_common.common import StreamingTestBase, counter_nodes, get_sensors
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for


class TestCleanup(StreamingTestBase):
    def test_cleanup(self, ydb_topics):
        io = ydb_topics('metrics_cleanup')
        query = io.start(f"""
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 1;
        """)
        timeout = plain_or_under_sanitizer(120, 500)

        def query_metrics():
            return {
                node_id: get_sensors(io.kikimr.cluster, node_id, 'kqp').find_sensors(
                    {'subsystem': 'streaming_queries', 'path': query}, key_label='sensor'
                )
                for node_id in counter_nodes(io.kikimr.cluster)
            }

        # Metrics appear after periodic statistics updates. Observe them before
        # finishing the query so the cleanup assertion cannot pass vacuously.
        assert wait_for(
            lambda: any(metrics.get('streaming.query.tasks.count', 0) > 0 for metrics in query_metrics().values()),
            timeout_seconds=timeout,
            step_seconds=0.5,
        ), query_metrics()

        io.write(['finish'])
        assert io.read(1) == ['finish']
        assert wait_for(
            lambda: io.state(query)['Status'] == 'COMPLETED',
            timeout_seconds=timeout,
            step_seconds=0.5,
        ), io.state(query)
        assert wait_for(
            lambda: all(not metrics for metrics in query_metrics().values()),
            timeout_seconds=timeout,
            step_seconds=0.5,
        ), query_metrics()

    def test_keep(self, ydb_topics):
        io = ydb_topics('metrics_keep')
        query = io.start(f"""
            PRAGMA ydb.OptValidateStreamingCheckpoints = 'FALSE';
            INSERT INTO {io.output} SELECT Data FROM {io.input} LIMIT 1;
        """)
        io.write(['finish'])
        assert io.read(1) == ['finish']
        assert wait_for(
            lambda: io.state(query)['Status'] == 'COMPLETED', timeout_seconds=120, step_seconds=0.5
        ), io.state(query)
        io.kikimr.ydb_client.query(f"""
            ALTER STREAMING QUERY `{query}` SET (
                RUN = TRUE, FORCE = TRUE, STREAMING_DISPOSITION = FRESH
            ) AS DO BEGIN
                INSERT INTO {io.output} SELECT Data FROM {io.input};
            END DO;
        """)
        io.checkpoint(query)

        def tasks():
            return sum(
                get_sensors(io.kikimr.cluster, node_id, 'kqp').find_sensor(
                    {'subsystem': 'streaming_queries', 'path': query, 'sensor': 'streaming.query.tasks.count'}
                )
                or 0
                for node_id in counter_nodes(io.kikimr.cluster)
            )

        assert wait_for(lambda: tasks() > 0, timeout_seconds=120, step_seconds=0.5), 'Tasks metric did not appear'
        # The old test covered a complete 60-second cleanup interval plus a margin.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert tasks() > 0, 'Tasks metric disappeared while the query was running'
            time.sleep(1)
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.write(['still_running'])
        assert io.read(1) == ['still_running']
        io.stop(query)
