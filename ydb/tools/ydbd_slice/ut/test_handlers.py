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
            host_dynamic_slot_counts={},
            host_storage_enabled={},
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
            host_storage_enabled={},
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
        content = """\
hosts:
- name: olap-1.search.yandex.net
  dynamic_slots: 1
- host: vla-1.search.yandex.net
  dynamic_slots: 2
- name: other.search.yandex.net
domains:
- domain_name: Root
  dynamic_slots: 2
static_erasure: none
"""
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
            host_dynamic_slot_counts={},
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
        content = """\
hosts:
- name: olap-1.search.yandex.net
  host_config_id: 1
- name: vla-1.search.yandex.net
  storage: false
static_erasure: none
"""
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

    def test_apply_storage_process_profiles_rewrites_kikimr_cfg(self):
        host = 'ydb-olap-perf-001.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={host: 'storage'},
            host_dynamic_profiles={},
        )
        configurator = SimpleNamespace(enable_process_profiles=True)

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['cfg']},
                nodes.Nodes([host]),
                cluster_details,
                configurator=configurator,
            )

        with mock.patch.object(slice_obj.nodes, 'execute_async') as execute_async:
            slice_obj._apply_storage_process_profiles()

        execute_async.assert_called_once()
        cmd = execute_async.call_args[0][0]
        self.assertIn('config.p_storage.yaml', cmd)
        self.assertIn('--yaml-config ${kikimr_config}/config.yaml', cmd)
        self.assertEqual(execute_async.call_args[1]['nodes'], [host])

    def test_deploy_slot_config_copies_profile_yaml(self):
        host = 'vla5-2569.search.yandex.net'
        slot = SimpleNamespace(
            slot='31003',
            domain='olap-perf',
            grpc=31001,
            ic=31003,
            mbus=31002,
            mon=31004,
            kafka_port=31005,
        )
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            domains=[SimpleNamespace(domain_name='olap-perf')],
            dynamic_slots=[slot],
            host_dynamic_slot_counts={host: 1},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={host: ['dyn-a']},
        )
        tenant = SimpleNamespace(name='db')

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([host]),
                cluster_details,
            )

        with mock.patch.object(slice_obj.nodes, 'execute_async') as execute_async:
            slice_obj._deploy_slot_config_for_tenant(slot, tenant, host)

        copy_call = execute_async.call_args_list[-1]
        self.assertIn('config.p_dyn-a.yaml', copy_call[0][0])
        self.assertIn('/Berkanavt/kikimr_31003/config.yaml', copy_call[0][0])
        self.assertEqual(copy_call[1]['nodes'], [host])
