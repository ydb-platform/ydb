# -*- coding: utf-8 -*-
import socket
import logging
from ydb.tests.library.nemesis.nemesis_core import NemesisProcess
from ydb.tests.library.nemesis.nemesis_network import NetworkNemesis
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
from ydb.tests.tools.nemesis.library.datacenter import datacenter_nemesis_list
from ydb.tests.tools.nemesis.library.bridge_pile import bridge_pile_nemesis_list


logger = logging.getLogger("catalog")
logger.setLevel(logging.DEBUG)
logger.propagate = True

# Временное логирование для диагностики
logger.info("=== CATALOG.PY LOADED ===")

def is_first_cluster_node(cluster):
    logger.info("Checking if current node is first cluster node")
    logger.info("cluster.hostnames: %s", cluster.hostnames)
    logger.info("socket.gethostname(): %s", socket.gethostname())
    if len(cluster.hostnames) > 0:
        logger.info("First cluster node (hostname): %s", cluster.hostnames[0])
        is_first = cluster.hostnames[0] == socket.gethostname().strip()
        logger.info("Is first cluster node: %s", is_first)
        return is_first
    logger.info("No hostnames in cluster")
    return False


def is_bridge_cluster(cluster):
    logger.info("Checking if cluster is bridge cluster")
    logger.info("cluster.yaml_config: %s", cluster.yaml_config)
    if cluster.yaml_config is not None:
        bridge_config = cluster.yaml_config.get('config', {}).get('bridge_config')
        logger.info("bridge_config: %s", bridge_config)
        if bridge_config is not None:
            logger.info("Cluster IS bridge cluster")
            return True
    logger.info("Cluster is NOT bridge cluster")
    return False


def basic_kikimr_nemesis_list(
        cluster, ssh_username, num_of_pq_nemesis=10, network_nemesis=False,
        enable_nemesis_list_filter_by_hostname=False):
    # Временное логирование для диагностики
    logger.info("=== basic_kikimr_nemesis_list CALLED ===")
    
    logger.info("Building nemesis list")
    logger.info("is_bridge_cluster: %s", is_bridge_cluster(cluster))
    logger.info("is_first_cluster_node: %s", is_first_cluster_node(cluster))
    
    harmful_nemesis_list = []
    logger.info("Adding data storage nemesis")
    data_storage_nemesis = data_storage_nemesis_list(cluster)
    logger.info("Data storage nemesis count: %d", len(data_storage_nemesis))
    harmful_nemesis_list.extend(data_storage_nemesis)
    
    logger.info("Adding nodes nemesis")
    nodes_nemesis = nodes_nemesis_list(cluster)
    logger.info("Nodes nemesis count: %d", len(nodes_nemesis))
    harmful_nemesis_list.extend(nodes_nemesis)
    
    logger.info("Adding tablet management nemesis")
    harmful_nemesis_list.extend(
        [
            KickTabletsFromNode(cluster),
            ReBalanceTabletsNemesis(cluster),
        ]
    )

    if network_nemesis:
        logger.info("Adding network nemesis")
        harmful_nemesis_list.append(
            NetworkNemesis(
                cluster,
                ssh_username=ssh_username
            )
        )

    if is_bridge_cluster(cluster):
        logger.info("Adding bridge pile nemesis")
        try:
            bridge_nemesis = bridge_pile_nemesis_list(cluster)
            logger.info("Bridge pile nemesis count: %d", len(bridge_nemesis))
            harmful_nemesis_list.extend(bridge_nemesis)
        except Exception as e:
            logger.error("Failed to add bridge pile nemesis: %s", e)
    else:
        logger.info("Adding datacenter nemesis")
        try:
            datacenter_nemesis = datacenter_nemesis_list(cluster)
            logger.info("Datacenter nemesis count: %d", len(datacenter_nemesis))
            harmful_nemesis_list.extend(datacenter_nemesis)
        except Exception as e:
            logger.error("Failed to add datacenter nemesis: %s", e)

    logger.info("Building light nemesis list")
    light_nemesis_list = []
    
    logger.info("Adding basic tablet nemesis")
    basic_tablet_nemesis = [
        KillCoordinatorNemesis(cluster),
        KillHiveNemesis(cluster),
        KillBsControllerNemesis(cluster),
        KillNodeBrokerNemesis(cluster),
        KillSchemeShardNemesis(cluster),
        KillMediatorNemesis(cluster),
        KillTxAllocatorNemesis(cluster),
        KillKeyValueNemesis(cluster),
        KillTenantSlotBrokerNemesis(cluster),
    ]
    logger.info("Basic tablet nemesis count: %d", len(basic_tablet_nemesis))
    light_nemesis_list.extend(basic_tablet_nemesis)

    logger.info("Adding change tablet group nemesis")
    change_tablet_nemesis = change_tablet_group_nemesis_list(cluster)
    logger.info("Change tablet group nemesis count: %d", len(change_tablet_nemesis))
    light_nemesis_list.extend(change_tablet_nemesis)
    
    logger.info("Adding %d KillPersQueueNemesis instances", num_of_pq_nemesis)
    light_nemesis_list.extend([KillPersQueueNemesis(cluster) for _ in range(num_of_pq_nemesis)])
    
    logger.info("Adding %d KillDataShardNemesis instances", num_of_pq_nemesis)
    light_nemesis_list.extend([KillDataShardNemesis(cluster) for _ in range(num_of_pq_nemesis)])
    
    logger.info("Adding %d KillBlocktoreVolume instances", num_of_pq_nemesis)
    light_nemesis_list.extend([KillBlocktoreVolume(cluster) for _ in range(num_of_pq_nemesis)])
    
    logger.info("Adding %d KillBlocktorePartition instances", num_of_pq_nemesis)
    light_nemesis_list.extend([KillBlocktorePartition(cluster) for _ in range(num_of_pq_nemesis)])

    logger.info("Final nemesis list assembly")
    logger.info("Total harmful nemesis count: %d", len(harmful_nemesis_list))
    logger.info("Total light nemesis count: %d", len(light_nemesis_list))
    
    nemesis_list = []
    if enable_nemesis_list_filter_by_hostname:
        logger.info("Using hostname-based nemesis filtering")
        hostnames = cluster.hostnames
        self_hostname = socket.gethostname()
        self_id = None

        for host_id, hostname in enumerate(hostnames):
            if self_hostname == hostname:
                self_id = host_id

        logger.info("Self hostname: %s, Self ID: %s", self_hostname, self_id)

        for nemesis_actor_id, nemesis_actor in enumerate(light_nemesis_list):
            if self_id is not None and nemesis_actor_id % len(hostnames) == self_id:
                nemesis_list.append(nemesis_actor)

        logger.info("Filtered light nemesis count: %d", len(nemesis_list))

        if is_first_cluster_node(cluster):
            logger.info("Adding harmful nemesis (first cluster node)")
            nemesis_list.extend(harmful_nemesis_list)
        else:
            logger.info("Skipping harmful nemesis (not first cluster node)")

        logger.info("Final nemesis list count: %d", len(nemesis_list))
        return nemesis_list
    
    logger.info("Adding all light nemesis")
    nemesis_list.extend(light_nemesis_list)
    logger.info("Adding all harmful nemesis")
    nemesis_list.extend(harmful_nemesis_list)

    logger.info("Final nemesis list count: %d", len(nemesis_list))
    return nemesis_list


def nemesis_factory(kikimr_cluster, ssh_username, num_of_pq_nemesis=10, **kwargs):
    logger.info("=== NEMESIS_FACTORY CALLED ===")
    logger.info("Creating nemesis factory")
    logger.info("Cluster: %s", kikimr_cluster)
    logger.info("SSH username: %s", ssh_username)
    logger.info("Num of PQ nemesis: %d", num_of_pq_nemesis)
    logger.info("Additional kwargs: %s", kwargs)
    
    nemesis_list = basic_kikimr_nemesis_list(kikimr_cluster, ssh_username, num_of_pq_nemesis, **kwargs)
    logger.info("Created nemesis list with %d items", len(nemesis_list))
    
    nemesis_process = NemesisProcess(nemesis_list)
    logger.info("Created NemesisProcess")
    return nemesis_process
