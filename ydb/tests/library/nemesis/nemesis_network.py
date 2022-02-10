# -*- coding: utf-8 -*-
import random

from ydb.tests.library.nemesis.nemesis_core import Nemesis
from ydb.tests.library.nemesis.network.client import NetworkClient


class NetworkNemesis(Nemesis):
    def __init__(self, cluster, schedule=60, ssh_username=None, max_number_affected_nodes=4, probability=0.5):
        super(NetworkNemesis, self).__init__(schedule)
        self.__cluster = cluster
        self.__ssh_username = ssh_username
        self.__max_number_affected_nodes = max_number_affected_nodes
        self.__probability = probability
        self.__node_ids_to_clients = {
            node_id: NetworkClient(node.host, port=node.ic_port, ssh_username=ssh_username)
            for node_id, node in cluster.nodes.items()
        }
        self.__cur_affected_nodes = 0

    def prepare_state(self):
        self.logger.info('Prepare state')

    def inject_fault(self):
        self.__cur_affected_nodes += 1
        if self.__cur_affected_nodes > self.__max_number_affected_nodes:
            self.extract_fault()
            return

        random_node_client = random.choice(self.__node_ids_to_clients.values())
        self.logger.info('Injecting fault with client = {client}'.format(client=random_node_client))
        random_node_client.isolate_node()

    def extract_fault(self):
        self.logger.info('Extracting all faults')
        for node_client in self.__node_ids_to_clients.values():
            node_client.clear_all_drops()
        self.__cur_affected_nodes = 0


class DnsNemesis(Nemesis):
    def __init__(self, cluster, schedule=60, ssh_username=None, max_number_affected_nodes=1, probability=0.5):
        super(DnsNemesis, self).__init__(schedule)
        self.__cluster = cluster
        self.__ssh_username = ssh_username
        self.__max_number_affected_nodes = max_number_affected_nodes
        self.__probability = probability
        self.__node_ids_to_clients = {
            node_id: NetworkClient(node.host, port=node.ic_port, ssh_username=ssh_username)
            for node_id, node in cluster.nodes.items()
        }
        self.__cur_affected_nodes = 0

    def prepare_state(self):
        self.logger.info('Prepare state')

    def inject_fault(self):
        self.__cur_affected_nodes += 1
        if self.__cur_affected_nodes > self.__max_number_affected_nodes:
            self.extract_fault()
            return

        random_node_client = random.choice(self.__node_ids_to_clients.values())
        self.logger.info('Injecting fault with client = {client}'.format(client=random_node_client))
        random_node_client.isolate_dns()

    def extract_fault(self):
        self.logger.info('Extracting all faults')
        for node_client in self.__node_ids_to_clients.values():
            node_client.clear_all_drops()
        self.__cur_affected_nodes = 0
