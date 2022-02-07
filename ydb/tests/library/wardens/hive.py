#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from ydb.tests.library.wardens.base import LivenessWarden

logger = logging.getLogger(__name__)


class BootQueueSizeWarden(LivenessWarden):
    def __init__(self, cluster):
        super(BootQueueSizeWarden, self).__init__()
        self.cluster = cluster

    @property
    def list_of_liveness_violations(self):
        hive_boot_queue_size = 0
        asked_hosts = 0

        monitors = {node_id: node.monitor for node_id, node in self.cluster.nodes.items()}
        for node_id, node in self.cluster.nodes.items():
            asked_hosts += 1
            hive_boot_queue_size += monitors[node_id].sensor(
                counters='tablets',
                type='Hive',
                sensor="SUM(Hive/BootQueueSize)",
                category='app',
                _default=0
            )

        if asked_hosts < 1:
            return [
                'Liveness violation: was not able ask Hive about tablets'
            ]

        if hive_boot_queue_size != 0:
            return [
                'Liveness violation for sensor: '
                'SUM(Hive/BootQueueSize) != 0, actual value is %d' % hive_boot_queue_size
            ]
        else:
            return []


class AllTabletsAliveLivenessWarden(LivenessWarden):
    def __init__(self, cluster):
        self.cluster = cluster

    @property
    def list_of_liveness_violations(self):
        tablets_alive_count, tablets_count, state_done = 0, 0, 0
        monitors = {node_id: node.monitor for node_id, node in self.cluster.nodes.items()}
        for node_id, node in self.cluster.nodes.items():
            tablets_alive_count += monitors[node_id].sensor(
                counters='tablets',
                type='Hive',
                sensor="SUM(Hive/TabletsAlive)",
                category='app',
                _default=0
            )
            tablets_count += monitors[node_id].sensor(
                counters='tablets',
                type='Hive',
                sensor="SUM(Hive/TabletsTotal)",
                category='app',
                _default=0
            )
            state_done += monitors[node_id].sensor(
                counters='tablets',
                type='Hive',
                sensor="SUM(Hive/StateDone)",
                category='app',
                _default=0
            )

        if tablets_alive_count != tablets_count or state_done != 1:
            return [
                'Liveness violation for Hive tablet counters: %d non-active tablet(s). '
                'tablets alive - %d, tablets total - %d, state done - %d' % (
                    tablets_count - tablets_alive_count,
                    tablets_alive_count, tablets_count, state_done
                )
            ]
        else:
            return []
