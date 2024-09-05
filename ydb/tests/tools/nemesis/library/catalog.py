# -*- coding: utf-8 -*-
import socket

from ydb.tests.library.nemesis.nemesis_core import NemesisProcess
from ydb.tests.library.nemesis.nemesis_network import NetworkNemesis

from ydb.tests.library.harness import param_constants

from ydb.tests.tools.nemesis.library.node import nodes_nemesis_list

from ydb.tests.tools.nemesis.library.tablet import change_tablet_group_nemesis_list
from ydb.tests.tools.nemesis.library.tablet import ReBalanceTabletsNemesis
from ydb.tests.tools.nemesis.library.tablet import KillTenantSlotBrokerNemesis
from ydb.tests.tools.nemesis.library.tablet import KillPersQueueNemesis
from ydb.tests.tools.nemesis.library.tablet import KickTabletsFromNode
from ydb.tests.tools.nemesis.library.tablet import KillKeyValueNemesis
from ydb.tests.tools.nemesis.library.tablet import KillHiveNemesis
from ydb.tests.tools.nemesis.library.tablet import KillBsControllerNemesis
from ydb.tests.tools.nemesis.library.tablet import KillCoordinatorNemesis
from ydb.tests.tools.nemesis.library.tablet import KillSchemeShardNemesis
from ydb.tests.tools.nemesis.library.tablet import KillMediatorNemesis
from ydb.tests.tools.nemesis.library.tablet import KillDataShardNemesis
from ydb.tests.tools.nemesis.library.tablet import KillTxAllocatorNemesis
from ydb.tests.tools.nemesis.library.tablet import KillNodeBrokerNemesis
from ydb.tests.tools.nemesis.library.tablet import KillBlocktoreVolume
from ydb.tests.tools.nemesis.library.tablet import KillBlocktorePartition
from ydb.tests.tools.nemesis.library.disk import data_storage_nemesis_list


def is_first_cluster_node(cluster):
    if len(cluster.hostnames) > 0:
        return cluster.hostnames[0] == socket.gethostname().strip()
    return False


def basic_kikimr_nemesis_list(
        cluster, num_of_pq_nemesis=10, network_nemesis=False,
        enable_nemesis_list_filter_by_hostname=False):
    harmful_nemesis_list = []
    harmful_nemesis_list.extend(data_storage_nemesis_list(cluster))
    harmful_nemesis_list.extend(nodes_nemesis_list(cluster))
    harmful_nemesis_list.extend(
        [
            KickTabletsFromNode(cluster),
            ReBalanceTabletsNemesis(cluster),
        ]
    )

    if network_nemesis:
        harmful_nemesis_list.append(
            NetworkNemesis(
                cluster,
                ssh_username=param_constants.ssh_username
            )
        )

    light_nemesis_list = []
    light_nemesis_list.extend([
        KillCoordinatorNemesis(cluster),
        KillHiveNemesis(cluster),
        KillBsControllerNemesis(cluster),
        KillNodeBrokerNemesis(cluster),
        KillSchemeShardNemesis(cluster),
        KillMediatorNemesis(cluster),
        KillTxAllocatorNemesis(cluster),
        KillKeyValueNemesis(cluster),
        KillTenantSlotBrokerNemesis(cluster),
    ])

    light_nemesis_list.extend(change_tablet_group_nemesis_list(cluster))
    light_nemesis_list.extend([KillPersQueueNemesis(cluster) for _ in range(num_of_pq_nemesis)])
    light_nemesis_list.extend([KillDataShardNemesis(cluster) for _ in range(num_of_pq_nemesis)])
    light_nemesis_list.extend([KillBlocktoreVolume(cluster) for _ in range(num_of_pq_nemesis)])
    light_nemesis_list.extend([KillBlocktorePartition(cluster) for _ in range(num_of_pq_nemesis)])

    nemesis_list = []
    if enable_nemesis_list_filter_by_hostname:
        hostnames = cluster.hostnames
        self_hostname = socket.gethostname()
        self_id = None

        for host_id, hostname in enumerate(hostnames):
            if self_hostname == hostname:
                self_id = host_id

        for nemesis_actor_id, nemesis_actor in enumerate(light_nemesis_list):
            if self_id is not None and nemesis_actor_id % len(hostnames) == self_id:
                nemesis_list.append(nemesis_actor)

        if is_first_cluster_node(cluster):
            nemesis_list.extend(
                harmful_nemesis_list)

        return nemesis_list
    nemesis_list.extend(light_nemesis_list)
    nemesis_list.extend(harmful_nemesis_list)
    return nemesis_list


def nemesis_factory(kikimr_cluster, num_of_pq_nemesis=10, **kwargs):
    return NemesisProcess(basic_kikimr_nemesis_list(kikimr_cluster, num_of_pq_nemesis, **kwargs))
