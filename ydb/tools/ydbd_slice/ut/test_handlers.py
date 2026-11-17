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

    def test_restart_after_update_uses_rolling_restart(self):
        host = 'ydb-olap-perf-001.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['bin']},
                nodes.Nodes([host]),
                cluster_details,
                rolling_restart_sec=60,
            )

        with mock.patch.object(slice_obj, '_deploy_slot_configs') as deploy_slots, \
                mock.patch.object(slice_obj, '_rolling_restart') as rolling, \
                mock.patch.object(slice_obj, 'slice_stop') as stop, \
                mock.patch.object(slice_obj, 'slice_start') as start:
            slice_obj._restart_after_update()

        deploy_slots.assert_called_once_with()
        rolling.assert_called_once_with(60)
        stop.assert_not_called()
        start.assert_not_called()

    def test_list_kikimr_pids_command_filters(self):
        static_cmd = handlers._static_pids_command()
        dynamic_cmd = handlers._dynamic_pids_command()
        self.assertIn('--node static', static_cmd)
        self.assertIn('--ic-port', dynamic_cmd)
        self.assertIn('!~ /--node static/', dynamic_cmd)
        self.assertEqual(handlers._pid_count('10 20\n30'), 3)
        self.assertEqual(handlers._parse_pids(' 11 22 '), ['11', '22'])

    def test_rolling_restart_seconds_validation(self):
        from ydb.tools.ydbd_slice import rolling_restart_seconds
        import argparse

        self.assertEqual(rolling_restart_seconds('0'), 0)
        self.assertEqual(rolling_restart_seconds('60'), 60)
        with self.assertRaises(argparse.ArgumentTypeError):
            rolling_restart_seconds('-1')
        with self.assertRaises(argparse.ArgumentTypeError):
            rolling_restart_seconds('x')

    def test_rolling_restart_storage_then_dynamic_with_pause(self):
        storage = 'storage-host.search.yandex.net'
        compute = 'compute-host.search.yandex.net'
        slot = SimpleNamespace(slot='31003', domain='Root')
        tenant = SimpleNamespace(name='db')
        cluster_details = SimpleNamespace(
            hosts=[
                SimpleNamespace(hostname=storage),
                SimpleNamespace(hostname=compute),
            ],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={compute: False},
            host_storage_profile={},
            host_dynamic_profiles={},
            dynamic_slots=[slot],
            domains=[],
        )

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['bin'], 'dynamic_slots': ['all']},
                nodes.Nodes([storage, compute]),
                cluster_details,
                rolling_restart_sec=5,
            )

        assignment = (slot, tenant, compute, None)
        order = []
        with mock.patch.object(
                slice_obj, '_rolling_restart_static_host',
                side_effect=lambda *a, **k: order.append('static')) as static_host, \
                mock.patch.object(
                    slice_obj, '_rolling_restart_dynamic_host',
                    side_effect=lambda *a, **k: order.append('dynamic')) as dynamic_host, \
                mock.patch.object(
                    slice_obj, '_stop_static_on_hosts',
                    side_effect=lambda *a, **k: order.append('stop_skip')) as stop_static, \
                mock.patch.object(slice_obj, '_iter_dynamic_assignments', return_value=[assignment]), \
                mock.patch.object(handlers.time, 'sleep') as sleep:
            slice_obj._rolling_restart(5)

        # storage:false hosts are stopped only after remaining storage has rolled.
        self.assertEqual(order, ['static', 'stop_skip', 'dynamic', 'dynamic'])
        stop_static.assert_called_once_with([compute])
        static_host.assert_called_once_with(storage, 1, 1)
        self.assertEqual(
            [call.args for call in dynamic_host.call_args_list],
            [
                (storage, 1, 2, []),
                (compute, 2, 2, [assignment]),
            ],
        )
        # Pause between storage/dynamic phases, then between the two dynamic hosts.
        self.assertEqual(sleep.call_args_list, [mock.call(5), mock.call(5)])

    def test_wait_kikimr_back_requires_old_pids_gone(self):
        host = 'host.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['bin']},
                nodes.Nodes([host]),
                cluster_details,
            )

        probes = [
            ('111', 0, ''),          # old still present
            ('111 222', 0, ''),      # old still present
            ('222', 0, ''),          # new only, poll 1
            ('222', 0, ''),          # poll 2
            ('222', 0, ''),          # poll 3 -> done
        ]

        def fake_probe(_host, _cmd):
            return probes.pop(0)

        with mock.patch.object(slice_obj, '_host_probe', side_effect=fake_probe), \
                mock.patch.object(handlers.time, 'sleep'), \
                mock.patch.object(handlers, 'ROLLING_RESTART_STABLE_POLLS', 3):
            slice_obj._wait_kikimr_back(
                host,
                'list-cmd',
                old_pids=['111'],
                min_count=1,
                phase='storage',
                index=1,
                total=1,
            )

        self.assertEqual(probes, [])

    def test_wait_kikimr_back_resets_when_pid_set_changes(self):
        host = 'host.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['bin']},
                nodes.Nodes([host]),
                cluster_details,
            )

        probes = [
            ('222', 0, ''),  # replacement #1
            ('333', 0, ''),  # different set -> reset
            ('333', 0, ''),
            ('333', 0, ''),  # stable -> done
        ]

        def fake_probe(_host, _cmd):
            return probes.pop(0)

        with mock.patch.object(slice_obj, '_host_probe', side_effect=fake_probe), \
                mock.patch.object(handlers.time, 'sleep'), \
                mock.patch.object(handlers, 'ROLLING_RESTART_STABLE_POLLS', 3):
            slice_obj._wait_kikimr_back(
                host,
                'list-cmd',
                old_pids=['111'],
                min_count=1,
                phase='storage',
                index=1,
                total=1,
            )

        self.assertEqual(probes, [])

    def test_rolling_restart_dynamic_host_uses_assignments_not_old_count(self):
        host = 'host.search.yandex.net'
        slot_old = SimpleNamespace(slot='31003', domain='Root')
        slot_new = SimpleNamespace(slot='31013', domain='Root')
        tenant = SimpleNamespace(name='db')
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
            dynamic_slots=[slot_old, slot_new],
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([host]),
                cluster_details,
            )

        assignment = (slot_new, tenant, host, None)
        waited = {}

        def fake_wait(_host, _cmd, old_pids, min_count, phase, index, total):
            waited['min_count'] = min_count
            waited['old_pids'] = old_pids

        with mock.patch.object(slice_obj, '_host_probe', return_value=('11 22 33', 0, '')), \
                mock.patch.object(slice_obj, '_stop_all_slots') as stop_all, \
                mock.patch.object(slice_obj, '_start_slot_for_tenant') as start_slot, \
                mock.patch.object(slice_obj, '_wait_kikimr_back', side_effect=fake_wait):
            slice_obj._rolling_restart_dynamic_host(host, 1, 1, [assignment])

        stop_all.assert_called_once_with(hosts=[host])
        start_slot.assert_called_once_with(slot_new, tenant, host=host, node_bind=None)
        self.assertEqual(waited['min_count'], 1)
        self.assertEqual(waited['old_pids'], ['11', '22', '33'])

    def test_rolling_probe_old_pids_fails_on_bad_retcode(self):
        host = 'host.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'kikimr': ['bin']},
                nodes.Nodes([host]),
                cluster_details,
            )

        with mock.patch.object(slice_obj, '_host_probe', return_value=('', 255, 'ssh failed')), \
                mock.patch.object(slice_obj.nodes, 'execute_async') as execute_async:
            with self.assertRaises(RuntimeError) as raised:
                slice_obj._rolling_restart_static_host(host, 1, 1)

        self.assertIn('failed to list kikimr pids', str(raised.exception))
        execute_async.assert_not_called()

    def test_rolling_restart_dynamic_drained_host_waits_for_old_pids(self):
        host = 'host.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
            dynamic_slots=[],
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([host]),
                cluster_details,
            )

        waited = {}

        def fake_wait(_host, _cmd, old_pids, min_count, phase, index, total):
            waited['min_count'] = min_count
            waited['old_pids'] = old_pids

        with mock.patch.object(slice_obj, '_host_probe', return_value=('11 22', 0, '')), \
                mock.patch.object(slice_obj, '_stop_all_slots') as stop_all, \
                mock.patch.object(slice_obj, '_start_slot_for_tenant') as start_slot, \
                mock.patch.object(slice_obj, '_wait_kikimr_back', side_effect=fake_wait):
            slice_obj._rolling_restart_dynamic_host(host, 1, 1, [])

        stop_all.assert_called_once_with(hosts=[host])
        start_slot.assert_not_called()
        self.assertEqual(waited['min_count'], 0)
        self.assertEqual(waited['old_pids'], ['11', '22'])

    def test_wait_kikimr_back_min_count_zero_waits_until_gone(self):
        host = 'host.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=host)],
            grpc_config={'port': 2135},
            host_dynamic_slot_counts={},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={},
        )
        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([host]),
                cluster_details,
            )

        probes = [
            ('11', 0, ''),  # still there
            ('', 0, ''),    # gone, poll 1
            ('', 0, ''),    # poll 2
            ('', 0, ''),    # poll 3
        ]

        with mock.patch.object(slice_obj, '_host_probe', side_effect=lambda *_: probes.pop(0)), \
                mock.patch.object(handlers.time, 'sleep'), \
                mock.patch.object(handlers, 'ROLLING_RESTART_STABLE_POLLS', 3):
            slice_obj._wait_kikimr_back(
                host,
                'list-cmd',
                old_pids=['11'],
                min_count=0,
                phase='dynamic',
                index=1,
                total=1,
            )

        self.assertEqual(probes, [])
