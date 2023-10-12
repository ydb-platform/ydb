# -*- coding: utf-8 -*-
import abc
import random
import logging

from ydb.tests.library.common.delayed import wait_tablets_are_active
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_client import kikimr_client_factory
from ydb.tests.library.kv.helpers import create_tablets_and_wait_for_start

TIMEOUT_SECONDS = 480
TABLETS_PER_NODE = 5
logger = logging.getLogger(__name__)


class AbstractLocalClusterTest(object):
    erasure = None

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(cls.erasure, use_in_memory_pdisks=False)
        cls.cluster = kikimr_cluster_factory(configurator=configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()


class AbstractTestRestartMultiple(AbstractLocalClusterTest):
    repetitions_count = 5

    @abc.abstractmethod
    def _select_nodes_to_stop(self):
        pass

    def test_tablets_are_successfully_started_after_few_killed_nodes(self):
        nodes_count = len(self.cluster.nodes.values())
        all_tablet_ids = create_tablets_and_wait_for_start(self.cluster.client, nodes_count * TABLETS_PER_NODE)

        for iteration_idx in range(1, self.repetitions_count + 1):
            logger.info("Starting %d iteration of nodes restart", iteration_idx)
            nodes_to_stop, nodes_to_keep = self._select_nodes_to_stop()

            for node in nodes_to_stop:
                node.stop()

            logger.info(
                "Stopped nodes, node_ids: %s", ",".join(
                    str(node.node_id) for node in nodes_to_stop
                )
            )

            client = kikimr_client_factory(nodes_to_keep[0].host, nodes_to_keep[0].grpc_port, retry_count=100)
            wait_tablets_are_active(client, all_tablet_ids, cluster=self.cluster)
            logger.info(
                "Starting nodes, node_ids: %s", ','.join(
                    str(node.node_id)
                    for node in nodes_to_stop
                )
            )
            for node in nodes_to_stop:
                node.start()

            wait_tablets_are_active(client, all_tablet_ids, cluster=self.cluster)


class AbstractTestRestartSingle(AbstractLocalClusterTest):
    def test_restart_single_node_is_ok(self):
        # Act
        num_of_nodes = len(self.cluster.nodes)
        all_tablet_ids = create_tablets_and_wait_for_start(self.cluster.client, num_of_nodes * TABLETS_PER_NODE)

        for node_id, node in self.cluster.nodes.items():
            if node_id == 1:
                continue

            logger.info("Starting iteration, node_id=%s, node=%s", node_id, str(node))
            node.stop()

            logger.info("Node is stopped %s", node_id)
            wait_tablets_are_active(self.cluster.client, all_tablet_ids)

            node.start()

            logger.info("Node is started %s", node_id)
            wait_tablets_are_active(self.cluster.client, all_tablet_ids)

        # Assert
        wait_tablets_are_active(
            self.cluster.client,
            all_tablet_ids,
        )


class AbstractTestRestartCluster(AbstractLocalClusterTest):
    def test_when_create_many_tablets_and_restart_cluster_then_every_thing_is_ok(self):
        # Act
        num_of_nodes = len(self.cluster.nodes)
        all_tablet_ids = create_tablets_and_wait_for_start(self.cluster.client, num_of_nodes * TABLETS_PER_NODE)

        for node_id, node in self.cluster.nodes.items():
            logger.info("Node is stopped %s", node_id)
            node.stop()

        for node_id, node in self.cluster.nodes.items():
            node.start()
            logger.info("Node is started %s", node_id)

        # Assert
        wait_tablets_are_active(self.cluster.client, all_tablet_ids)


class TestRestartSingleBlock42(AbstractTestRestartSingle):
    erasure = Erasure.BLOCK_4_2


class TestRestartSingleMirror3DC(AbstractTestRestartSingle):
    erasure = Erasure.MIRROR_3_DC


class TestRestartClusterBlock42(AbstractTestRestartCluster):
    erasure = Erasure.BLOCK_4_2


class TestRestartClusterMirror3DC(AbstractTestRestartCluster):
    erasure = Erasure.MIRROR_3_DC


class TestRestartClusterMirror34(AbstractTestRestartCluster):
    erasure = Erasure.MIRROR_3OF4


class TestRestartMultipleMirror34(AbstractTestRestartMultiple):
    erasure = Erasure.MIRROR_3OF4

    def _select_nodes_to_stop(self):
        nodes = list(self.cluster.nodes.values())
        random.shuffle(nodes)
        return nodes[:2], nodes[2:]


class TestRestartMultipleBlock42(AbstractTestRestartMultiple):
    erasure = Erasure.BLOCK_4_2

    def _select_nodes_to_stop(self):
        nodes = list(self.cluster.nodes.values())
        random.shuffle(nodes)
        return nodes[:2], nodes[2:]


class TestRestartMultipleMirror3DC(AbstractTestRestartMultiple):
    erasure = Erasure.MIRROR_3_DC

    def _select_nodes_to_stop(self):
        nodes = list(self.cluster.nodes.values())

        dc_nodes = {}
        for node in nodes:
            node_id = node.node_id
            dc_id = (node_id - 1) % 3 + 1
            if dc_id not in dc_nodes:
                dc_nodes[dc_id] = []
            dc_nodes[dc_id].append(node)

        chosen_dc = random.choice(list(dc_nodes.keys()))

        nodes_to_stop = dc_nodes[chosen_dc]
        nodes_to_keep = []
        for dc in dc_nodes.keys():
            if dc != chosen_dc:
                nodes_to_keep.extend(dc_nodes[dc])

        return nodes_to_stop, nodes_to_keep
