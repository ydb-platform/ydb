"""Observe operator-managed pile membership without restarting Skipper."""

import json
import signal
import time

from contextlib import closing
from urllib.parse import urlsplit

import grpc
import yaml

from test_skipper import RealSkipperCluster, TOKEN, eventually, state_rpc
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as bridge_grpc
from ydb.public.api.protos import ydb_config_pb2 as config
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.clients.kikimr_config_client import ConfigClient


class TestSkipperPileMembership(RealSkipperCluster):
    erasure = Erasure.NONE
    nodes_count = 6
    use_in_memory_pdisks = True

    def configure_cluster(self):
        super().configure_cluster()
        ids = {
            self.configurator.port_allocator.get_node_port_allocator(i).ic_port: i
            for i in range(1, self.nodes_count + 1)
        }
        for host in self.configurator.yaml_config['hosts']:
            host['node_id'] = ids[host['port']]

    def fetch_config(self, via):
        node = self.pile_nodes[via][0]
        with closing(ConfigClient(node.host, node.port, timeout=10)) as client:
            client.set_auth_token(TOKEN[0][1])
            response = client.fetch_all_configs()
        assert response.operation.status == StatusIds.SUCCESS, response
        result = config.FetchConfigResult()
        assert response.operation.result.Unpack(result) and len(result.config) == 1, response
        cfg = yaml.safe_load(result.config[0].config)
        cfg['metadata']['version'] += 1
        return cfg

    def replace_config(self, cfg, via, nodes):
        node = self.pile_nodes[via][0]
        with closing(ConfigClient(node.host, node.port, timeout=30)) as client:
            client.set_auth_token(TOKEN[0][1])
            response = client.replace_config(yaml.safe_dump(cfg))
        assert response.operation.status == StatusIds.SUCCESS, response

        def applied():
            return all(node.read_node_config()['metadata']['version'] == cfg['metadata']['version'] for node in nodes)

        eventually(applied, 'updated membership configuration', timeout=60)

    def pile_operation(self, name, remove=False, via='r1'):
        request = bridge.RemovePileRequest(pile_name=name) if remove else bridge.AddPileRequest(pile_name=name)
        request.operation_params.operation_timeout.seconds = 60
        with grpc.insecure_channel(self.pile_nodes[via][0].endpoint) as channel:
            stub = bridge_grpc.BridgeServiceStub(channel)
            response = (stub.RemovePile if remove else stub.AddPile)(request, metadata=TOKEN, timeout=65)
        assert response.operation.status == StatusIds.SUCCESS, response

    def add_pile(self, name, via='r1', nodes_count=3):
        cfg = self.fetch_config(via)
        section = cfg['config']
        section['bridge_config']['piles'].append(dict(name=name))
        first = max(self.cluster.nodes) + 1
        for node_id in range(first, first + nodes_count):
            host_id = max(h['host_config_id'] for h in section['host_configs']) + 1
            section['host_configs'].append(
                dict(host_config_id=host_id, drive=[dict(path='SectorMap:%d:64' % node_id, type='ROT')])
            )
            section['hosts'].append(
                dict(
                    node_id=node_id,
                    host='localhost',
                    port=self.configurator.port_allocator.get_node_port_allocator(node_id).ic_port,
                    host_config_id=host_id,
                    location=dict(bridge_pile_name=name),
                )
            )
        old_nodes = list(self.cluster.nodes.values())
        self.configurator.full_config = cfg
        new_nodes = [self.cluster.prepare_node(self.configurator) for _ in range(nodes_count)]
        self.replace_config(cfg, via, old_nodes)
        for node in new_nodes:
            node.start()
        self.pile_nodes[name] = new_nodes
        self.wait_snapshot(
            lambda s: s['view'] and any(p['name'] == name and p['state'] == 'INACTIVE' for p in s['view']['piles']),
            'new inactive pile',
        )
        self.pile_operation(name, via=via)
        self.wait_snapshot(
            lambda s: s['view'] and any(p['name'] == name and p['state'] == 'DISCONNECTED' for p in s['view']['piles']),
            'provisioned pile',
        )
        self.guarded_update({name: PileState.NOT_SYNCHRONIZED}, via=via)

        def synchronized():
            return any(
                p.pile_name == name and p.state == PileState.SYNCHRONIZED
                for p in state_rpc(self.pile_nodes[via][0], connectivity=False).pile_states
            )

        eventually(synchronized, 'new pile synchronization', timeout=180)
        return new_nodes

    def remove_pile(self, name, via):
        self.guarded_update({name: PileState.DISCONNECTED}, via=via)
        self.pile_operation(name, remove=True, via=via)
        self.wait_snapshot(
            lambda s: s['view'] and any(p['name'] == name and p['state'] == 'INACTIVE' for p in s['view']['piles']),
            'destroyed inactive pile',
        )
        removed = self.pile_nodes[name]
        cfg = self.fetch_config(via)
        ports = {self.configurator.port_allocator.get_node_port_allocator(node.node_id).ic_port for node in removed}
        cfg['config']['hosts'] = [h for h in cfg['config']['hosts'] if h['port'] not in ports]
        cfg['config']['bridge_config']['piles'] = [
            p for p in cfg['config']['bridge_config']['piles'] if p['name'] != name
        ]
        remaining = {host['node_id'] for host in cfg['config']['hosts']}
        live = [node for node in self.cluster.nodes.values() if node.node_id in remaining]
        self.replace_config(cfg, via, live)
        self.kill_pile(name)
        return self.wait_snapshot(
            lambda s: s['view']
            and all(p['name'] != name for p in s['view']['piles'])
            and all(e['pile'] != name for e in s['endpoints']),
            'removed pile retired',
        )

    def test_add_remove_seed_and_fail_over_to_new_pile(self):
        self.create_data()
        before = self.start_skipper(('r1',))
        boot = before['controller_boot_id']
        self.skipper.mode('auto')
        self.add_pile('r3')
        self.wait_snapshot(
            lambda s: any(e['pile'] == 'r3' and e['identity'] == 'VERIFIED' for e in s['endpoints']),
            'automatic discovery of new pile',
            timeout=40,
        )
        self.wait_snapshot(
            lambda s: any(c['name'] == 'r3' and c['verdict'] == 'HEALTHY' for c in s['cards']),
            'new pile health',
            timeout=90,
        )
        self.check_data('r3')
        self.guarded_update({'r1': PileState.DISCONNECTED, 'r2': PileState.PRIMARY}, via='r2')
        state = self.remove_pile('r1', via='r2')
        assert state['controller_boot_id'] == boot and state['mode'] == 'auto', state
        assert state['last_mutation'] is None, state
        # Every initial seed is now removed. Observation and mutation must use
        # the endpoints learned while this same controller process was running.
        self.wait_snapshot(
            lambda s: any(c['name'] == 'r3' and c['verdict'] == 'HEALTHY' for c in s['cards']),
            'post-role-change warm-up',
            timeout=90,
        )
        generation = int(self.skipper.state()['view']['generation'])
        started = time.monotonic()
        self.kill_pile('r2')
        expected = {'r2': 'DISCONNECTED', 'r3': 'PRIMARY'}
        state = self.wait_snapshot(
            lambda s: s['view'] and {p['name']: p['state'] for p in s['view']['piles']} == expected,
            'failover to newly added pile',
            timeout=max(0, 20 - (time.monotonic() - started)),
        )
        assert time.monotonic() - started <= 20, state
        assert state['controller_boot_id'] == boot and state['mode'] == 'auto', state
        assert int(state['last_mutation']['expected_generation']) == generation, state
        assert state['last_mutation']['update'] == expected, state
        self.check_data('r3')
        self.write_after_fault('r3')
        records = [json.loads(line) for line in (self.skipper.directory / 'skipper.log').read_text().splitlines()]
        assert len([r for r in records if r.get('event') == 'mutation_sent']) == 1, records

    def test_cached_discovery_recovers_after_all_views_expire(self):
        self.create_data()
        before = self.start_skipper(
            ('r1',), timing=dict(read_timeout_seconds=7, evidence_ttl_seconds=9), health_endpoints_per_pile=1
        )
        self.skipper.mode('auto')
        # Five nodes preserve the remaining pile's quorum while both installed
        # locators (one health client and at most one inventory client) are paused.
        self.add_pile('r3', nodes_count=5)
        self.wait_snapshot(
            lambda s: any(c['name'] == 'r3' and c['verdict'] == 'HEALTHY' for c in s['cards']),
            'new pile health', timeout=90,
        )
        self.guarded_update({'r1': PileState.DISCONNECTED, 'r3': PileState.PRIMARY}, via='r3')
        self.remove_pile('r1', via='r3')
        self.remove_pile('r2', via='r3')
        self.wait_snapshot(
            lambda s: any(c['name'] == 'r3' and c['verdict'] == 'HEALTHY' for c in s['cards']),
            'remaining pile health', timeout=90,
        )
        state = self.skipper.state()
        addresses = {e['uri'] for e in state['endpoints'] if e['pile'] == 'r3'}
        ports = {urlsplit(address).port for address in addresses}
        paused = [node for node in self.pile_nodes['r3'] if node.port in ports]
        assert 1 <= len(paused) <= 2, state
        live = [node for node in self.pile_nodes['r3'] if node not in paused]
        assert len(live) >= 3, state
        # Use a survivor for later data operations. SIGSTOP makes RPCs reach
        # their real client deadlines without destroying the cluster's quorum.
        self.pile_nodes['r3'] = live + paused
        for node in paused:
            self.cleanup.callback(node.send_signal, signal.SIGCONT)
            node.send_signal(signal.SIGSTOP)
        state = self.wait_snapshot(lambda s: s['view'] is None, 'all selected views expired', timeout=20)
        assert state['decision']['reason'] == 'NO_VIEW' and state['last_mutation'] is None, state
        state = self.wait_snapshot(
            lambda s: s['view'] and any(
                e['pile'] == 'r3' and e['identity'] == 'VERIFIED' and e['uri'] not in addresses
                for e in s['endpoints']
            ),
            'recovery through a cached locator', timeout=60,
        )
        assert state['controller_boot_id'] == before['controller_boot_id'], state
        assert state['mode'] == 'auto' and state['last_mutation'] is None, state
        # Erasure.NONE may keep table parts on the paused nodes. Recovery of
        # observation is proved above while they are still unavailable.
        for node in paused:
            node.send_signal(signal.SIGCONT)
        self.check_data('r3')
        self.write_after_fault('r3')

    def test_remove_pile_without_restarting_controller(self):
        self.create_data()
        before = self.start_skipper()
        self.skipper.mode('auto')
        state = self.remove_pile('r2', via='r1')
        assert state['controller_boot_id'] == before['controller_boot_id'], state
        assert state['mode'] == 'auto' and state['last_mutation'] is None, state
        self.check_data('r1')
        self.write_after_fault('r1')
