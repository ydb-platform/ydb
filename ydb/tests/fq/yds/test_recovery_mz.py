#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.fq.streaming_common.common import StreamingTestBase, get_checkpoint_coordinator_metric
from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.wait_for import wait_for


class TestRecovery(StreamingTestBase):
    @pytest.mark.parametrize('ydb_cluster', [{'distributed_compute': True}], indirect=True)
    def test_recovery(self, ydb_topics):
        io = ydb_topics('pq_kikimr_streaming_mz', partitions=2)
        query = io.start(f"""
            PRAGMA ydb.MaxTasksPerStage = '2';
            INSERT INTO {io.output} SELECT Data FROM {io.input};
        """)
        expected_compute_actors = sum(
            io.get_actor_count(io.kikimr, node_id, 'KQP_COMPUTE_ACTOR') for node_id in io.kikimr.cluster.slots
        )
        assert expected_compute_actors > 0
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 1)
        timeout = plain_or_under_sanitizer(120, 500)

        def coordinator_node():
            # This is the checkpoint coordinator activity in both YQ and KQP.
            # Select the live coordinator rather than inferring its role from workers.
            nodes = [
                node_id
                for node_id in io.kikimr.cluster.slots
                if self.get_actor_count(io.kikimr, node_id, 'YQ_CHECKPOINT_COORDINATOR') > 0
            ]
            return nodes[0] if len(nodes) == 1 else None

        def find_coordinator():
            assert wait_for(
                lambda: coordinator_node() is not None,
                timeout_seconds=timeout,
                step_seconds=0.5,
            ), 'No unique live checkpoint coordinator found'
            return coordinator_node()

        def check_batch(start, end):
            io.wait_actor_count('KQP_COMPUTE_ACTOR', expected_compute_actors)
            io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 1)
            expected = list(range(start, end))
            io.write([str(value) for value in expected])
            actual = [int(value) for value in io.read(len(expected))]
            assert sorted(actual) == expected, actual
            io.checkpoint(query, count=3)

        coordinator = find_coordinator()
        other_node = next(
            node_id
            for node_id in io.kikimr.cluster.slots
            if node_id != coordinator and self.get_actor_count(io.kikimr, node_id, 'KQP_COMPUTE_ACTOR') > 0
        )
        check_batch(1, 11)

        # First lose a compute node outside the coordinator, then the current
        # coordinator itself. Both failures must retain the saved input offsets.
        self.restart_node(io.kikimr, other_node)
        io.kikimr.wait_kqp_node_count(2)
        io.checkpoint(query, count=3)
        check_batch(11, 21)

        coordinator = find_coordinator()
        self.restart_node(io.kikimr, coordinator)
        io.kikimr.wait_kqp_node_count(2)
        io.checkpoint(query, count=3)
        check_batch(21, 31)

        restored = get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'RestoredFromSavedCheckpoint')
        started_empty = get_checkpoint_coordinator_metric(io.kikimr.cluster, query, 'StartedFromEmptyCheckpoint')
        assert restored >= 1, (restored, started_empty)
        assert io.state(query)['Status'] == 'RUNNING', io.state(query)
        io.stop(query)
        io.wait_actor_count('KQP_COMPUTE_ACTOR', 0)
        io.wait_actor_count('YQ_CHECKPOINT_COORDINATOR', 0)
