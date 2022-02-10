# -*- coding: utf-8 -*-
import six.moves

from ydb.tests.library.wardens.base import LivenessWarden
from ydb.core.protos import counters_schemeshard_pb2 as counters

SS_COUNTER_PREFIX = 'COUNTER_IN_FLIGHT_OPS_Tx'


class SchemeShardHasNoInFlightTransactions(LivenessWarden):
    def __init__(self, cluster):
        super(SchemeShardHasNoInFlightTransactions, self).__init__()
        self._cluster = cluster

    @property
    def list_of_liveness_violations(self):
        violations = []
        in_fly_stats = {}
        for counter_name, _ in counters.ESimpleCounters.items():
            if SS_COUNTER_PREFIX not in counter_name:
                continue

            transaction_type_name = counter_name.replace(SS_COUNTER_PREFIX, '')
            if 'CreateTableIndex' in transaction_type_name:
                continue

            sensor_name = "SUM(SchemeShard/InFlightOps/%s)" % transaction_type_name
            in_fly_stats[sensor_name] = []

        monitors = [node.monitor.fetch() for node in self._cluster.nodes.values()]
        for monitor in monitors:
            for sensor_name in sorted(in_fly_stats.keys()):
                sensor_value = monitor.sensor(
                    counters='tablets', type='SchemeShard', sensor=sensor_name, category='app')

                if sensor_value is not None:
                    in_fly_stats[sensor_name].append(sensor_value)

        no_data = set()
        total_sensors = 0
        for sensor_name, values in six.iteritems(in_fly_stats):
            total_sensors += 1
            if len(values) == 0:
                no_data.add(sensor_name)

            if sum(values) != 0:
                violations.append(
                    "Liveness violation for sensor %s: "
                    "SchemeShard has %d in flight transaction(s)." % (
                        sensor_name,
                        sum(values),
                    )
                )

        if len(no_data) == total_sensors:
            violations.append(
                "Liveness violation for SchemeShard sensors: "
                "failed to collect information about in-fly transactions in "
                "SchemeShard."
            )

        return violations
