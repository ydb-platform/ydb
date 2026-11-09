import ipaddress
import json
import socket
import time
import unittest

import requests

from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestHeartbeat(unittest.TestCase):
    def start_cluster(self, family=socket.AF_INET6, hostname_only=False):
        config = KikimrConfigGenerator(
            nodes=1,
            erasure=Erasure.NONE,
            enable_audit_log=True,
            audit_log_config={
                'file_backend': {'format': 'JSON_LOG_COMPATIBLE'},
                'log_class_config': [
                    {'log_class': 'AuditHeartbeat', 'enable_logging': True},
                ],
                'heartbeat': {'interval_seconds': 1},
            },
        )
        node_config = config.yaml_config['nameservice_config']['node'][0]
        if hostname_only:
            node_config.pop('address')
        else:
            address = socket.getaddrinfo('localhost', 0, family, socket.SOCK_STREAM)[0][4][0]
            node_config['address'] = address
            node_config['interconnect_host'] = address

        cluster = KiKiMR(configurator=config)
        self.addCleanup(cluster.stop)
        cluster.start()
        return cluster

    def node_identity(self, node):
        response = requests.get(
            f'http://{node.host}:{node.mon_port}/viewer/json/sysinfo',
            params={'node_id': 0},
            timeout=30,
        )
        response.raise_for_status()
        info, = response.json()['SystemStateInfo']
        return int(info['NodeId']), info['Host']

    def connected_addresses(self, host, port, family):
        # Obtain the peer IP from real connections to ydbd, independently of audit/config.
        addresses = set()
        for af, socktype, proto, _, endpoint in socket.getaddrinfo(host, port, family, socket.SOCK_STREAM):
            with socket.socket(af, socktype, proto) as connection:
                connection.settimeout(10)
                connection.connect(endpoint)
                addresses.add(ipaddress.ip_address(connection.getpeername()[0]))
        self.assertTrue(addresses)
        return addresses

    def check_heartbeat(self, cluster, node, host, family):
        node_id, _ = self.node_identity(node)
        addresses = self.connected_addresses(host, node.ic_port, family)
        deadline = time.monotonic() + plain_or_under_sanitizer(30, 120)
        records = []
        while time.monotonic() < deadline:
            with open(cluster.config.audit_file_path) as audit_file:
                records = []
                for line in audit_file:
                    if not line.endswith('\n'):
                        break
                    record = json.loads(line)
                    if record.get('operation') == 'HEARTBEAT' and int(record['node_id']) == node_id:
                        records.append(record)
            if len(records) >= 2:
                break
            time.sleep(0.1)

        self.assertGreaterEqual(len(records), 2, f'No repeated heartbeat for node {node_id}: {records}')
        for record in records:
            self.assertEqual(record['component'], 'audit-service')
            self.assertEqual(record['status'], 'SUCCESS')
            self.assertIn(ipaddress.ip_address(record['remote_address']), addresses, record)

    def test_static_node_ipv4(self):
        cluster = self.start_cluster(socket.AF_INET)
        node = cluster.nodes[1]
        self.check_heartbeat(cluster, node, node.host, socket.AF_INET)

    def test_static_node_ipv6(self):
        cluster = self.start_cluster(socket.AF_INET6)
        node = cluster.nodes[1]
        self.check_heartbeat(cluster, node, node.host, socket.AF_INET6)

    def test_static_node_hostname(self):
        cluster = self.start_cluster(hostname_only=True)
        node = cluster.nodes[1]
        self.check_heartbeat(cluster, node, node.host, socket.AF_INET6)

    def test_dynamic_node(self):
        cluster = self.start_cluster()
        database = '/Root/heartbeat'
        cluster.create_database(database, storage_pool_units_count={'hdd': 1})
        node, = cluster.register_and_start_slots(database, count=1)
        cluster.wait_tenant_up(database)
        _, host = self.node_identity(node)
        self.check_heartbeat(cluster, node, host, socket.AF_UNSPEC)
