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
        monitors = nodes_monitors + slots_monitors

        total_tx_complete_lag = 0
        readable = 0
        for monitor in monitors:
            if not monitor.has_actual_data():
                continue
            readable += 1
            sensors = monitor.get_by_name('SUM(DataShard/TxCompleteLag)')
            total_tx_complete_lag += sum(map(lambda x: x[1], sensors))

        if monitors and readable == 0:
            return [
                "Liveness violation for sensor SUM(DataShard/TxCompleteLag): "
                "failed to collect transaction completion lag."
            ]

        if total_tx_complete_lag != 0:
            return [
                "Liveness violation for sensor SUM(DataShard/TxCompleteLag): "
                "actual value is %d, but expected to be %d" % (
                    total_tx_complete_lag,
                    0
                )
            ]

        return []
