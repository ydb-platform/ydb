#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.library.wardens.base import LivenessWarden


class TxCompleteLagLivenessWarden(LivenessWarden):
    def __init__(self, cluster, counters=None):
        self._cluster = cluster
        self._counters = counters

    def _endpoints(self):
        return list(self._cluster.nodes.values()) + list(self._cluster.slots.values())

    def _readable_monitor(self, endpoint):
        if self._counters is None:
            monitor = endpoint.monitor
            if not monitor.has_actual_data():
                return None
            return monitor
        if not self._counters.readable(endpoint):
            return None
        return self._counters.monitor(endpoint)

    @property
    def list_of_liveness_violations(self):
        endpoints = self._endpoints()

        total_tx_complete_lag = 0
        readable = 0
        for endpoint in endpoints:
            monitor = self._readable_monitor(endpoint)
            if monitor is None:
                continue
            readable += 1
            sensors = monitor.get_by_name('SUM(DataShard/TxCompleteLag)')
            total_tx_complete_lag += sum(map(lambda x: x[1], sensors))

        if endpoints and readable == 0:
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
