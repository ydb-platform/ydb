from types import SimpleNamespace
from unittest import TestCase, mock

from ydb.tools.ydbd_slice import cluster_description
from ydb.tools.ydbd_slice import handlers
from ydb.tools.ydbd_slice import nodes


class SliceTest(TestCase):
    def test_config_client_uses_selected_host(self):
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname='unavailable-host')],
            grpc_config={'port': 2135},
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient') as config_client:
            handlers.Slice({}, nodes.Nodes(['selected-host']), cluster_details)

        config_client.assert_called_once_with('selected-host', 2135, retry_count=10)

    def test_available_slots_honor_per_host_dynamic_slots(self):
        slot1 = SimpleNamespace(slot='31003', domain='olap-perf')
        slot2 = SimpleNamespace(slot='31013', domain='olap-perf')
        olap = 'ydb-olap-perf-001.search.yandex.net'
        vla = 'vla5-2569.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=olap)],
            grpc_config={'port': 2135},
            domains=[SimpleNamespace(domain_name='olap-perf')],
            dynamic_slots=[slot1, slot2],
            host_dynamic_slot_counts={
                olap: 1,
                vla: 2,
            },
        )
        walle_provider = SimpleNamespace(get_datacenter=lambda hostname: 'FAKE')

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([olap, vla]),
                cluster_details,
                walle_provider=walle_provider,
            )

        slots_per_domain, total = slice._get_available_slots()
        any_zone = list(slots_per_domain['olap-perf']['any'])

        self.assertEqual(total, 3)
        self.assertEqual(
            [(item[0].slot, item[1]) for item in any_zone],
            [
                ('31003', olap),
                ('31003', vla),
                ('31013', vla),
            ],
        )

    def test_host_dynamic_slot_counts_from_yaml(self):
        content = (
            "hosts:\n"
            "- name: olap-1.search.yandex.net\n"
            "  dynamic_slots: 1\n"
            "- host: vla-1.search.yandex.net\n"
            "  dynamic_slots: 2\n"
            "- name: other.search.yandex.net\n"
            "domains:\n"
            "- domain_name: Root\n"
            "  dynamic_slots: 2\n"
            "static_erasure: none\n"
        )
        with mock.patch(
            'builtins.open',
            mock.mock_open(read_data=content),
        ):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            details.host_dynamic_slot_counts,
            {
                'olap-1.search.yandex.net': 1,
                'vla-1.search.yandex.net': 2,
            },
        )

    def test_start_static_skips_hosts_with_storage_false(self):
        olap = 'ydb-olap-perf-001.search.yandex.net'
        vla = 'vla5-2569.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=olap)],
            grpc_config={'port': 2135},
            host_storage_enabled={
                vla: False,
            },
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice = handlers.Slice(
                {'kikimr': ['bin', 'cfg']},
                nodes.Nodes([olap, vla]),
                cluster_details,
            )

        with mock.patch.object(slice.nodes, 'execute_async') as execute_async:
            slice._start_static()

        execute_async.assert_has_calls([
            mock.call("sudo service kikimr stop", check_retcode=False, nodes=[vla]),
            mock.call("sudo service kikimr start", check_retcode=True, nodes=[olap]),
        ])

    def test_host_storage_enabled_from_yaml(self):
        content = (
            "hosts:\n"
            "- name: olap-1.search.yandex.net\n"
            "  host_config_id: 1\n"
            "- name: vla-1.search.yandex.net\n"
            "  storage: false\n"
            "static_erasure: none\n"
        )
        with mock.patch(
            'builtins.open',
            mock.mock_open(read_data=content),
        ):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            details.host_storage_enabled,
            {
                'vla-1.search.yandex.net': False,
            },
        )
