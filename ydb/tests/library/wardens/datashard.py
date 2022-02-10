#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.library.wardens.base import LivenessWarden


class TxCompleteLagLivenessWarden(LivenessWarden):
    def __init__(self, cluster):
        self._cluster = cluster

    @property
    def list_of_liveness_violations(self):

        nodes_monitors = [node.monitor for node in self._cluster.nodes.values()]
        slots_monitors = [slot.monitor for slot in self._cluster.slots.values()]

        total_tx_complete_lag = 0
        for monitor in nodes_monitors + slots_monitors:
            sensors = monitor.get_by_name('SUM(DataShard/TxCompleteLag)')
            total_tx_complete_lag += sum(map(lambda x: x[1], sensors))

        if total_tx_complete_lag != 0:
            return [
                "Liveness violation for sensor SUM(DataShard/TxCompleteLag): "
                "actual value is %d, but expected be %d" % (
                    total_tx_complete_lag,
                    0
                )
            ]

        return []
