# -*- coding: utf-8 -*-
from ydb.tests.library.wardens.base import LivenessWarden


# Aggregated app counters of a PQ tablet. Published as
# counters=tablets, type=PQ, category=app.
PQ_TX_SENSORS = (
    "SUM(PQ/TxCompleteLag)",
    "SUM(PQ/TxInFly)",
)


class PersQueueHasNoStuckTransactions(LivenessWarden):
    """PQ distributed transactions must not stay in flight.

    TxCompleteLag is the age of the oldest transaction in the tablet queue,
    TxInFly is the number of distributed transactions. Both are zero when the
    queue is empty. Counters exist only on nodes that host a PQ tablet, so a
    cluster without topics is not a violation.
    """

    def __init__(self, cluster, counters=None):
        super(PersQueueHasNoStuckTransactions, self).__init__()
        self._cluster = cluster
        self._counters = counters

    def _endpoints(self):
        endpoints = []
        for attr in ("nodes", "slots"):
            group = getattr(self._cluster, attr, None)
            if group:
                endpoints.extend(group.values())
        return endpoints

    @property
    def list_of_liveness_violations(self):
        violations = []
        totals = {}
        for sensor_name in PQ_TX_SENSORS:
            totals[sensor_name] = []

        endpoints = self._endpoints()
        readable = 0
        for endpoint in endpoints:
            if self._counters is None:
                monitor = endpoint.monitor
                if not monitor.has_actual_data():
                    continue
            else:
                if not self._counters.readable(endpoint):
                    continue
                monitor = self._counters.monitor(endpoint)
            readable += 1
            for sensor_name in PQ_TX_SENSORS:
                sensor_value = monitor.sensor(
                    counters="tablets",
                    type="PQ",
                    sensor=sensor_name,
                    category="app",
                )
                if sensor_value is not None:
                    totals[sensor_name].append(sensor_value)

        if endpoints and readable == 0:
            violations.append(
                "Liveness violation for PQ sensors: "
                "failed to collect PQ/TxCompleteLag and PQ/TxInFly."
            )
            return violations

        for sensor_name in PQ_TX_SENSORS:
            values = totals[sensor_name]
            if len(values) == 0:
                continue
            total = sum(values)
            if total != 0:
                violations.append(
                    "Liveness violation for sensor %s: "
                    "actual value is %d, but expected to be %d." % (
                        sensor_name,
                        total,
                        0,
                    )
                )

        return violations
