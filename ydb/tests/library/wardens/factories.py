# -*- coding: utf-8 -*-
from ydb.tests.library.nemesis.safety_warden import AggregateSafetyWarden

from ydb.tests.library.wardens.base import AggregateLivenessWarden
from ydb.tests.library.wardens.datashard import TxCompleteLagLivenessWarden
from ydb.tests.library.wardens.logs import kikimr_grep_dmesg_safety_warden_factory
from ydb.tests.library.wardens.logs import kikimr_start_logs_safety_warden_factory
from ydb.tests.library.wardens.logs import kikimr_crit_and_alert_logs_safety_warden_factory
from ydb.tests.library.wardens.disk import AllPDisksAreInValidStateSafetyWarden
from ydb.tests.library.wardens.hive import AllTabletsAliveLivenessWarden, BootQueueSizeWarden
from ydb.tests.library.wardens.schemeshard import SchemeShardHasNoInFlightTransactions


def safety_warden_factory(cluster):
    list_of_host_names = [node.host for node in cluster.nodes.values()]
    wardens = [AllPDisksAreInValidStateSafetyWarden(cluster)]
    wardens.extend(kikimr_grep_dmesg_safety_warden_factory(list_of_host_names))
    by_directory = {}
    for node in list(cluster.slots.values()) + list(cluster.nodes.values()):
        if node.logs_directory not in by_directory:
            by_directory[node.logs_directory] = []
        by_directory[node.logs_directory].append(node.host)

    for directory, list_of_host_names in by_directory.items():
        wardens.extend(
            kikimr_start_logs_safety_warden_factory(
                list_of_host_names, directory
            )
        )

    return AggregateSafetyWarden(wardens)


def liveness_warden_factory(cluster):
    return AggregateLivenessWarden(
        [
            AllTabletsAliveLivenessWarden(cluster),
            BootQueueSizeWarden(cluster),
            SchemeShardHasNoInFlightTransactions(cluster),
            TxCompleteLagLivenessWarden(cluster)
        ]
    )


def strict_safety_warden_factory(cluster):
    list_of_host_names = [node.host for node in cluster.nodes.values()]
    return AggregateSafetyWarden(
        kikimr_crit_and_alert_logs_safety_warden_factory(list_of_host_names)
    )


def hive_liveness_warden_factory(cluster):
    return AggregateLivenessWarden(
        [
            AllTabletsAliveLivenessWarden(cluster),
            BootQueueSizeWarden(cluster),
        ]
    )


def transactions_processing_liveness_warden(cluster):
    return AggregateLivenessWarden(
        [
            SchemeShardHasNoInFlightTransactions(cluster),
            TxCompleteLagLivenessWarden(cluster),
        ]
    )
