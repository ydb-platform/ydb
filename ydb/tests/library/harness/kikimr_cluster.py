#!/usr/bin/env python
# -*- coding: utf-8 -*-
import itertools
import logging
import subprocess
import time
import pprint
from concurrent import futures

import ydb.tests.library.common.yatest_common as yatest_common
import ydb

from . import param_constants
from .kikimr_runner import KiKiMR, KikimrExternalNode
from .kikimr_cluster_interface import KiKiMRClusterInterface
from . import blockstore
import yaml

logger = logging.getLogger(__name__)

DEFAULT_INTERCONNECT_PORT = 19001
DEFAULT_MBUS_PORT = 2134
DEFAULT_MON_PORT = 8765
DEFAULT_GRPC_PORT = 2135


def kikimr_cluster_factory(configurator=None, config_path=None):
    logger.info("All test params = {}".format(pprint.pformat(yatest_common.get_param_dict_copy())))
    logger.info("Starting standalone YDB cluster")
    if config_path is not None:
        return ExternalKiKiMRCluster(config_path)
    else:
        return KiKiMR(configurator)


def load_yaml(path):
    with open(path, 'r') as r:
        data = yaml.safe_load(r.read())
    return data


class ExternalKiKiMRCluster(KiKiMRClusterInterface):
    def __init__(self, config_path, binary_path=None, output_path=None):
        self.__config_path = config_path
        self.__yaml_config = load_yaml(config_path)
        self.__hosts = [host['name'] for host in self.__yaml_config.get('hosts')]
        self._slots = None
        self._nbs = None
        self.__binary_path = binary_path if binary_path is not None else param_constants.kikimr_driver_path()
        self.__output_path = output_path
        self.__slot_count = 0

        for domain in self.__yaml_config['domains']:
            self.__slot_count = max(self.__slot_count, domain['dynamic_slots'])

        super(ExternalKiKiMRCluster, self).__init__()

    @property
    def config(self):
        return self.__config

    def add_storage_pool(self, erasure=None):
        raise NotImplementedError()

    def start(self):
        self._prepare_cluster()

        self._start()
        return self

    def stop(self):
        return self

    def restart(self):
        self._stop()
        self._start()
        return self

    def _start(self):
        for inst_set in [self.nodes, self.slots, self.nbs]:
            self._run_on(inst_set, lambda x: x.start())

    def _stop(self):
        for inst_set in [self.nodes, self.slots, self.nbs]:
            self._run_on(inst_set, lambda x: x.stop())

    @staticmethod
    def _run_on(instances, *funcs):
        with futures.ThreadPoolExecutor(8) as executor:
            for func in funcs:
                results = executor.map(
                    func,
                    instances.values()
                )
                # raising exceptions here if they occured
                # also ensure that funcs[0] finished before func[1]
                for _ in results:
                    pass

    def _deploy_secrets(self):
        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                "sudo mkdir -p /Berkanavt/kikimr/secrets && "
                "yav get version ver-01dsxdr7ghq7cnn7mvm66gqkxq -o auth_file | "
                "sudo tee /Berkanavt/kikimr/secrets/auth.txt"
            )
        )

        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                "yav get version ver-01dsxdr7ghq7cnn7mvm66gqkxq -o tvm_secret | "
                "sudo tee /Berkanavt/kikimr/secrets/tvm_secret"
            )
        )

    def _initialize(self):
        node = list(self.nodes.values())[0]
        node.ssh_command(['bash /Berkanavt/kikimr/cfg/init_storage.bash'], raise_on_error=True)
        node.ssh_command(['bash /Berkanavt/kikimr/cfg/init_cms.bash'], raise_on_error=True)
        node.ssh_command(['bash /Berkanavt/kikimr/cfg/init_compute.bash'], raise_on_error=True)
        node.ssh_command(['bash /Berkanavt/kikimr/cfg/init_databases.bash'])

    def _prepare_cluster(self):
        self._stop()

        self._deploy_secrets()

        for inst_set in [self.nodes, self.nbs]:
            self._run_on(
                inst_set,
                lambda x: x.prepare_artifacts(
                    self.__config_path
                )
            )

        # creating symlinks, to attach auth.txt to node
        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                "sudo ln -f /Berkanavt/kikimr/secrets/auth.txt /Berkanavt/kikimr/cfg/auth.txt"
            )
        )

        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                'echo "Keys { ContainerPath: \'"\'/Berkanavt/kikimr/cfg/fake-secret.txt\'"\' Pin: \'"\' \'"\' Id: \'"\'fake-secret\'"\' Version: 1 }" | '
                'sudo tee /Berkanavt/kikimr/cfg/key.txt',
            )
        )

        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                "echo \"simple text\" | "
                "sudo tee /Berkanavt/kikimr/cfg/fake-secret.txt"
            )
        )

        self._run_on(
            self.nodes,
            lambda x: x.ssh_command(
                "sudo chown root:root /Berkanavt/kikimr/cfg/key.txt && sudo chown root:root /Berkanavt/kikimr/cfg/fake-secret.txt"
            )
        )

        if param_constants.deploy_cluster:
            for inst_set in [self.nodes, self.slots, self.nbs]:
                self._run_on(
                    inst_set,
                    lambda x: x.cleanup_logs()
                )

            self._run_on(
                self.nodes,
                lambda x: x.cleanup_disks(),
                lambda x: x.start()
            )

            time.sleep(5)

            self._initialize()

        self._start()

    @property
    def nodes(self):
        return {
            node_id: KikimrExternalNode(
                node_id=node_id,
                host=host,
                port=DEFAULT_GRPC_PORT,
                mon_port=DEFAULT_MON_PORT,
                ic_port=DEFAULT_INTERCONNECT_PORT,
                mbus_port=DEFAULT_MBUS_PORT,
                configurator=None,
            ) for node_id, host in zip(itertools.count(start=1), self.__hosts)
        }

    @property
    def nbs(self):
        return {}
        if self._nbs is None:
            self._nbs = {}
            for node_id, host in zip(itertools.count(start=1), self.__hosts):
                self._nbs[node_id] = blockstore.ExternalNetworkBlockStoreNode(host)
        return self._nbs

    @property
    def slots(self):
        if self._slots is None:
            self._slots = {}
            slot_count = self.__slot_count
            slot_idx_allocator = itertools.count(start=1)
            for node_id in sorted(self.nodes.keys()):
                node = self.nodes[node_id]
                start = 31000
                for node_slot_id in range(1, slot_count + 1):
                    slot_idx = next(slot_idx_allocator)
                    mbus_port = start + 0
                    grpc_port = start + 1
                    mon_port = start + 2
                    ic_port = start + 3

                    self._slots[slot_idx] = KikimrExternalNode(
                        node_id=node_id,
                        host=node.host,
                        port=grpc_port,
                        mon_port=mon_port,
                        ic_port=ic_port,
                        mbus_port=mbus_port,
                        configurator=None,
                        slot_id=node_slot_id,
                    )
                    start += 10

        return self._slots

    def _run_discovery_command(self, tenant_name):
        discovery_cmd = [
            self.__binary_path, '--server', self.nodes[1].host, 'discovery', 'list', '-d', tenant_name
        ]
        logger.info('Executing discovery command: %s' % ' '.join(list(discovery_cmd)))
        ds_result = subprocess.check_output(discovery_cmd)
        logger.info('Discovery command result: "{}"'.format(ds_result))
        return ds_result

    def _get_node(self, host, grpc_port):
        nodes = self._slots if self._slots else self.nodes
        for node in nodes.values():
            if node.host == host and str(node.grpc_port) == str(grpc_port):
                return node
        logger.error('Cant find node with host {} and grpc_port {}'.format(host, grpc_port))

    def get_active_tenant_nodes(self, tenant_name):
        node_to_connect_to = self.nodes[1]
        dc = ydb.DriverConfig('%s:%d' % (node_to_connect_to.host, node_to_connect_to.grpc_port), tenant_name)
        resolver = ydb.DiscoveryEndpointsResolver(dc)
        for try_num in range(5):
            resolve_result = resolver.resolve()
            if resolve_result is None:
                logger.error('Got None result from DiscoveryEndpointsResolver')
                time.sleep(1)
                continue
            endpoints = resolve_result.endpoints
            logger.info(
                'DiscoveryEndpointsResolver returned {} nodes: {}'.format(len(endpoints), endpoints)
            )
            nodes = []
            for endpoint in endpoints:
                node = self._get_node(endpoint.address, endpoint.port)
                if node is not None:
                    nodes.append(node)
            return nodes
