import os
import tempfile
from types import SimpleNamespace
from unittest import TestCase, mock

import yaml

from ydb.tools.ydbd_slice import cluster_description
from ydb.tools.ydbd_slice import handlers
from ydb.tools.ydbd_slice import nodes
from ydb.tools.ydbd_slice import process_profiles
from ydb.tools.ydbd_slice import yaml_configurator


class DeepMergeTest(TestCase):
    def test_nested_maps_merge_lists_replace(self):
        base = {
            'composite_conveyor_config': {
                'worker_pools': [{'name': 'INSRT', 'workers_count': 1}],
                'keep': True,
            },
            'other': 1,
        }
        overlay = {
            'composite_conveyor_config': {
                'worker_pools': [{'name': 'SCAN', 'workers_count': 30}],
            },
        }
        merged = process_profiles.deep_merge(base, overlay)
        self.assertEqual(merged['other'], 1)
        self.assertTrue(merged['composite_conveyor_config']['keep'])
        self.assertEqual(
            merged['composite_conveyor_config']['worker_pools'],
            [{'name': 'SCAN', 'workers_count': 30}],
        )


class StripTest(TestCase):
    def test_strips_root_and_host_slice_keys(self):
        cfg = {
            'process_profiles': [{'id': 'x'}],
            'hosts': [
                {
                    'host': 'h.search.yandex.net',
                    'storage': False,
                    'dynamic_slots': 2,
                    'storage_profile': 's',
                    'dynamic_profiles': ['a', 'b'],
                    'location': {'rack': '1'},
                }
            ],
            'actor_system_config': {'executor': []},
        }
        stripped = process_profiles.strip_slice_only_fields(cfg)
        self.assertNotIn('process_profiles', stripped)
        host = stripped['hosts'][0]
        self.assertEqual(host['host'], 'h.search.yandex.net')
        self.assertEqual(host['location'], {'rack': '1'})
        for key in process_profiles.HOST_SLICE_ONLY_KEYS:
            self.assertNotIn(key, host)
        self.assertIn('actor_system_config', stripped)


class CatalogValidationTest(TestCase):
    def test_duplicate_ids_error(self):
        with self.assertRaises(ValueError):
            process_profiles.parse_process_profiles({
                'process_profiles': [{'id': 'a'}, {'id': 'a'}],
            })

    def test_unknown_storage_profile_error(self):
        template = {
            'hosts': [{'name': 'h', 'storage_profile': 'missing'}],
            'process_profiles': [{'id': 'storage'}],
        }
        catalog = process_profiles.parse_process_profiles(template)
        with self.assertRaises(ValueError):
            process_profiles.validate_host_profiles(template, catalog, domain_slot_count=2)

    def test_storage_profile_forbidden_when_storage_false(self):
        template = {
            'hosts': [{'name': 'h', 'storage': False, 'storage_profile': 'storage'}],
            'process_profiles': [{'id': 'storage'}],
        }
        catalog = process_profiles.parse_process_profiles(template)
        with self.assertRaises(ValueError):
            process_profiles.validate_host_profiles(template, catalog, domain_slot_count=1)

    def test_dynamic_profiles_length_must_match_dynamic_slots(self):
        template = {
            'hosts': [{'name': 'h', 'dynamic_slots': 1, 'dynamic_profiles': ['a', 'b']}],
            'process_profiles': [{'id': 'a'}, {'id': 'b'}],
        }
        catalog = process_profiles.parse_process_profiles(template)
        with self.assertRaises(ValueError):
            process_profiles.validate_host_profiles(template, catalog, domain_slot_count=2)

    def test_dynamic_profiles_length_matches_domain_slots_when_omitted(self):
        template = {
            'hosts': [{'name': 'h', 'dynamic_profiles': ['a', 'b']}],
            'process_profiles': [{'id': 'a'}, {'id': 'b'}],
        }
        catalog = process_profiles.parse_process_profiles(template)
        process_profiles.validate_host_profiles(template, catalog, domain_slot_count=2)

    def test_integer_ids_are_allowed(self):
        template = {
            'hosts': [{'name': 'h', 'storage_profile': 1, 'dynamic_profiles': [2]}],
            'process_profiles': [{'id': 1}, {'id': 2}],
        }
        catalog = process_profiles.parse_process_profiles(template)
        process_profiles.validate_host_profiles(template, catalog, domain_slot_count=1)
        self.assertEqual(process_profiles.profile_yaml_filename(1), 'config.p_1.yaml')
        self.assertEqual(
            process_profiles.host_storage_profiles(template),
            {'h': '1'},
        )
        self.assertEqual(
            process_profiles.host_dynamic_profiles(template),
            {'h': ['2']},
        )

    def test_require_flag_if_used(self):
        template = {'process_profiles': [{'id': 'a'}]}
        with self.assertRaises(ValueError):
            process_profiles.require_flag_if_used(template, enabled=False)
        process_profiles.require_flag_if_used(template, enabled=True)
        process_profiles.require_flag_if_used({'hosts': []}, enabled=False)


class SysConvertTest(TestCase):
    def test_compact_sys_becomes_actor_system_config(self):
        overlay = {
            'id': 'dyn-b',
            'sys': {
                'executors': {
                    'system': {'threads': 4, 'spin_threshold': 1},
                    'user': {'threads': 23, 'spin_threshold': 1},
                    'ic': {'threads': 6},
                }
            },
        }
        prepared = process_profiles.prepare_overlay(overlay)
        self.assertNotIn('sys', prepared)
        by_name = {item['name']: item for item in prepared['actor_system_config']['executor']}
        self.assertEqual(by_name['System']['threads'], 4)
        self.assertEqual(by_name['User']['threads'], 23)
        self.assertEqual(by_name['IC']['threads'], 6)


class EmitFilesTest(TestCase):
    def test_writes_stripped_base_and_merged_profile_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = os.path.join(tmp, 'config.yaml')
            base = {
                'hosts': [{'host': 'h.search.yandex.net', 'storage': False, 'dynamic_slots': 2}],
                'composite_conveyor_config': {
                    'worker_pools': [{'name': 'INSRT', 'workers_count': 1}],
                },
                'process_profiles': [
                    {
                        'id': 'dyn-a',
                        'composite_conveyor_config': {
                            'worker_pools': [{'name': 'SCAN', 'workers_count': 30}],
                        },
                    }
                ],
            }
            with open(config_path, 'w') as f:
                yaml.safe_dump(base, f)

            written = process_profiles.emit_profile_yaml_files(config_path, base)
            self.assertIn('dyn-a', written)

            with open(config_path) as f:
                stripped = yaml.safe_load(f)
            self.assertNotIn('process_profiles', stripped)
            self.assertNotIn('storage', stripped['hosts'][0])

            with open(written['dyn-a']) as f:
                merged = yaml.safe_load(f)
            self.assertEqual(
                merged['composite_conveyor_config']['worker_pools'][0]['name'],
                'SCAN',
            )
            self.assertNotIn('storage', merged['hosts'][0])


class ClusterDetailsProfilesTest(TestCase):
    def test_host_profile_maps_from_yaml(self):
        content = """\
hosts:
- name: olap-1.search.yandex.net
  dynamic_slots: 1
  storage_profile: storage
  dynamic_profiles: [dyn-a]
- host: vla-1.search.yandex.net
  storage: false
  dynamic_slots: 2
  dynamic_profiles: [dyn-a, dyn-b]
process_profiles:
- id: storage
- id: dyn-a
- id: dyn-b
domains:
- domain_name: Root
  dynamic_slots: 2
static_erasure: none
"""
        with mock.patch('builtins.open', mock.mock_open(read_data=content)):
            details = cluster_description.ClusterDetails('cluster.yaml')

        self.assertEqual(
            details.host_storage_profile,
            {'olap-1.search.yandex.net': 'storage'},
        )
        self.assertEqual(
            details.host_dynamic_profiles,
            {
                'olap-1.search.yandex.net': ['dyn-a'],
                'vla-1.search.yandex.net': ['dyn-a', 'dyn-b'],
            },
        )


class SlotIndexProfileTest(TestCase):
    def test_domain_slot_index_maps_left_to_right_profiles(self):
        slot1 = SimpleNamespace(slot='31003', domain='olap-perf', grpc=31001, ic=31003)
        slot2 = SimpleNamespace(slot='31013', domain='olap-perf', grpc=31011, ic=31013)
        olap = 'ydb-olap-perf-001.search.yandex.net'
        vla = 'vla5-2569.search.yandex.net'
        cluster_details = SimpleNamespace(
            hosts=[SimpleNamespace(hostname=olap)],
            grpc_config={'port': 2135},
            domains=[SimpleNamespace(domain_name='olap-perf')],
            dynamic_slots=[slot1, slot2],
            host_dynamic_slot_counts={olap: 1, vla: 2},
            host_storage_enabled={},
            host_storage_profile={},
            host_dynamic_profiles={
                olap: ['dyn-a'],
                vla: ['dyn-a', 'dyn-b'],
            },
        )
        walle_provider = SimpleNamespace(get_datacenter=lambda hostname: 'FAKE')

        with mock.patch.object(handlers.config_client, 'ConfigClient'):
            slice_obj = handlers.Slice(
                {'dynamic_slots': ['all']},
                nodes.Nodes([olap, vla]),
                cluster_details,
                walle_provider=walle_provider,
            )

        self.assertEqual(slice_obj._profile_id_for_slot(vla, slot1), 'dyn-a')
        self.assertEqual(slice_obj._profile_id_for_slot(vla, slot2), 'dyn-b')
        self.assertEqual(slice_obj._profile_id_for_slot(olap, slot1), 'dyn-a')
        self.assertIsNone(slice_obj._profile_id_for_slot(olap, slot2))


class YamlConfiguratorEmitTest(TestCase):
    def test_create_static_cfg_emits_profile_files_when_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = os.path.join(tmp, 'src-config.yaml')
            databases_path = os.path.join(tmp, 'databases.yaml')
            out_dir = os.path.join(tmp, 'out')
            os.mkdir(out_dir)
            config = {
                'static_erasure': 'none',
                'hosts': [
                    {
                        'host': 'h.search.yandex.net',
                        'dynamic_slots': 1,
                        'dynamic_profiles': ['dyn-a'],
                    }
                ],
                'actor_system_config': {
                    'executor': [{'name': 'System', 'type': 'BASIC', 'threads': 1}],
                },
                'process_profiles': [
                    {
                        'id': 'dyn-a',
                        'actor_system_config': {
                            'executor': [{'name': 'System', 'type': 'BASIC', 'threads': 4}],
                        },
                    }
                ],
            }
            with open(config_path, 'w') as f:
                yaml.safe_dump(config, f)
            with open(databases_path, 'w') as f:
                yaml.safe_dump({'domains': [{'domain_name': 'Root', 'dynamic_slots': 1}]}, f)

            configurator = yaml_configurator.YamlConfigurator(
                databases_path, out_dir, config_path
            )
            configurator.enable_process_profiles = True
            configurator.create_static_cfg()

            with open(os.path.join(out_dir, 'config.yaml')) as f:
                base = yaml.safe_load(f)
            self.assertNotIn('process_profiles', base)
            self.assertNotIn('dynamic_profiles', base['hosts'][0])

            profile_path = os.path.join(out_dir, 'config.p_dyn-a.yaml')
            self.assertTrue(os.path.isfile(profile_path))
            with open(profile_path) as f:
                profile = yaml.safe_load(f)
            self.assertEqual(profile['actor_system_config']['executor'][0]['threads'], 4)

    def test_v2_config_rejects_process_profiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = os.path.join(tmp, 'src-config.yaml')
            databases_path = os.path.join(tmp, 'databases.yaml')
            out_dir = os.path.join(tmp, 'out')
            os.mkdir(out_dir)
            config = {
                'metadata': {'kind': 'MainConfig', 'cluster': 'test', 'version': 0},
                'config': {
                    'static_erasure': 'none',
                    'hosts': [{'host': 'h.search.yandex.net'}],
                },
                'process_profiles': [{'id': 'dyn-a'}],
            }
            with open(config_path, 'w') as f:
                yaml.safe_dump(config, f)
            with open(databases_path, 'w') as f:
                yaml.safe_dump({'domains': [{'domain_name': 'Root', 'dynamic_slots': 1}]}, f)

            configurator = yaml_configurator.YamlConfigurator(
                databases_path, out_dir, config_path
            )
            configurator.enable_process_profiles = True
            with self.assertRaises(ValueError):
                configurator.create_static_cfg()


class DynamicCfgTemplateTest(TestCase):
    def test_prefers_slot_local_config_yaml(self):
        from ydb.tools.cfg.templates import dynamic_cfg_new_style
        cfg = dynamic_cfg_new_style()
        self.assertIn('${tenant_main_dir}/config.yaml', cfg)
        self.assertIn('${kikimr_config}/config.yaml', cfg)
