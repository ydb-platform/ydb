import copy
import json
import shutil
import subprocess
import unittest
from unittest import mock

from ydb.core.protos import config_pb2
from ydb.tools.ydb_bench.lib import cluster_config, cluster_config_ui
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class ClusterConfigTest(unittest.TestCase):
    def test_complete_descriptor_graph(self):
        schema = cluster_config.schema()
        json.dumps(schema, allow_nan=False)
        ephemeral = cluster_config.yaml_config_pb2.TEphemeralInputFields.DESCRIPTOR
        pending, seen = [config_pb2.TAppConfig.DESCRIPTOR, ephemeral], set()
        while pending:
            message = pending.pop()
            if message.full_name in seen:
                continue
            seen.add(message.full_name)
            fields = schema['messages'][message.full_name]
            expected = list(message.fields)
            if message is config_pb2.TAppConfig.DESCRIPTOR:
                expected += list(ephemeral.fields)
            self.assertEqual([field.name for field in expected], [field['proto_name'] for field in fields])
            self.assertEqual(len(fields), len({field['name'] for field in fields}))
            pending.extend(field.message_type for field in message.fields if field.message_type)
        self.assertEqual(seen, set(schema['messages']))

    def test_yaml_input_fields_are_typed_and_roundtrip(self):
        config = {
            'hosts': [
                {'host': 'example.test', 'host_config_id': 1, 'location': {'data_center': 'dc', 'rack': 'r', 'body': 1}}
            ],
            'host_configs': [{'host_config_id': 1, 'drive': [{'path': '/dev/disk/by-partlabel/ydb', 'type': 'SSD'}]}],
            'erasure': 'none',
            'fail_domain_type': 'rack',
            'default_disk_type': 'SSD',
            'storage_pool_types': [{'kind': 'ssd', 'pool_config': {'erasure_species': 'none', 'kind': 'ssd'}}],
        }
        self.assertEqual((config, []), cluster_config.validate(config))
        result = cluster_config.document_response(config, {}, [])
        self.assertEqual(config, cluster_config.parse_document(result['yaml'], [])['config'])
        for bad in [{'hosts': 'text'}, {'host_configs': [{'host_config_id': -1}]}, {'fail_domain_type': 'invalid'}]:
            with self.subTest(bad=bad), self.assertRaises(BenchmarkError):
                cluster_config.validate(bad)
        with self.assertRaisesRegex(BenchmarkError, 'host_configs'):
            cluster_config.execution_config(config)

    def test_yaml_erasure_aliases_and_execution_guards(self):
        for key in ('erasure', 'static_erasure'):
            config = {key: 'block-4-2'}
            self.assertEqual('block-4-2', cluster_config.erasure_name(cluster_config.execution_config(config)))
        with self.assertRaisesRegex(BenchmarkError, 'Conflicting erasure'):
            cluster_config.execution_config(
                {'erasure': 'none', 'self_management_config': {'erasure_species': 'block-4-2'}}
            )
        with self.assertRaisesRegex(BenchmarkError, 'fail_domain_type'):
            cluster_config.execution_config({'fail_domain_type': 'body'})
        with self.assertRaisesRegex(BenchmarkError, 'invalid domain name'):
            cluster_config.execution_config({'domain_name': '../Root'})
        for config in [{'erasure': 'none'}, {'default_disk_type': 'SSD'}, {'storage_pool_types': []}]:
            with self.subTest(config=config), self.assertRaisesRegex(BenchmarkError, 'Tenant selectors'):
                cluster_config.tenant_configs({'/Root/a': config}, ['/Root/a'], execution=True)

    def test_names_match_dense_snake_case(self):
        for original, expected in [('PDiskInfo', 'pdisk_info'), ('GRpcConfig', 'grpc_config'), ('UDFsDir', 'udfs_dir')]:
            self.assertEqual(expected, cluster_config.yaml_name(original))

    def test_v2_document_roundtrip_and_tenant_isolation(self):
        base = {'feature_flags': {'enable_system_views': True}}
        overrides = {'/Root/a': {'feature_flags': {'enable_system_views': False}}, '/Root/b': {}}
        paths = list(overrides)
        result = cluster_config.document_response(base, overrides, paths)
        self.assertIn('metadata:', result['yaml'])
        self.assertIn('selector_config:', result['yaml'])
        self.assertIn('feature_flags: !inherit', result['yaml'])
        parsed = cluster_config.parse_document(result['yaml'], paths)
        self.assertEqual(base, parsed['config'])
        self.assertEqual(overrides, parsed['tenant_configs'])
        self.assertEqual(overrides, cluster_config.tenant_configs(overrides, paths, execution=True))
        with self.assertRaises(BenchmarkError):
            cluster_config.tenant_configs({'/Root/a': {'actor_system_config': {}}}, paths, execution=True)
        self.assertEqual({}, cluster_config.parse_document('feature_flags: {}', paths)['tenant_configs'])

    def test_v2_document_rejects_ambiguous_or_unsupported_selectors(self):
        prefix = 'metadata: {kind: MainConfig, cluster: "", version: 0}\nconfig: {}\nselector_config:\n'
        for selector in [
            '- {selector: {tenant: /Root/missing}, config: {}}',
            '- {selector: {node_id: 1}, config: {}}',
            '- {selector: {tenant: /Root/a}, config: {}}\n- {selector: {tenant: /Root/a}, config: {}}',
            '- {selector: {tenant: /Root/a}, config: !unsafe {}}',
        ]:
            with self.subTest(selector=selector), self.assertRaises(BenchmarkError):
                cluster_config.parse_document(prefix + selector, ['/Root/a'])

    def test_tenant_replacement_roundtrip(self):
        overrides = {'/Root/a': {'feature_flags': {'enable_system_views': False}, 'future': {'nested': {}}}}
        replacements = {'/Root/a': [['feature_flags'], ['future', 'nested']]}
        result = cluster_config.document_response({}, overrides, list(overrides), replacements)
        self.assertIn('feature_flags:\n', result['yaml'])
        self.assertIn('future: !inherit', result['yaml'])
        self.assertIn('nested: {}', result['yaml'])
        parsed = cluster_config.parse_document(result['yaml'], list(overrides))
        self.assertEqual(overrides, parsed['tenant_configs'])
        self.assertEqual(replacements, parsed['tenant_replacements'])
        for invalid in [
            False,
            {'/Root/missing': []},
            {'/Root/a': [[]]},
            {'/Root/a': [['missing']]},
            {'/Root/a': [['feature_flags'], ['feature_flags']]},
            {'/Root/a': [['feature_flags', 'enable_system_views']]},
        ]:
            with self.subTest(invalid=invalid), self.assertRaises(BenchmarkError):
                cluster_config.document_response({}, overrides, list(overrides), invalid)

    def test_presence_unknown_and_precision_roundtrip(self):
        value = {'actor_system_config': {'use_auto_config': False}, 'future_section': {'limit': 2**64 - 1}}
        result = cluster_config.response(value)
        self.assertEqual(['future_section'], result['unknown'])
        self.assertEqual({'use_auto_config': False}, result['config']['actor_system_config'])
        self.assertEqual(str(2**64 - 1), result['config']['future_section']['limit'])
        self.assertEqual(result['config'], cluster_config.parse(result['yaml'])[0])
        self.assertEqual({}, cluster_config.validate({})[0])

    def test_invalid_types_and_limits(self):
        for value in ['false', 0, None, [], {}]:
            with self.subTest(value=value), self.assertRaises(BenchmarkError):
                cluster_config.validate({'actor_system_config': {'use_auto_config': value}})
        for value in [-1, 2**32, 1.5, True]:
            with self.subTest(value=value), self.assertRaises(BenchmarkError):
                cluster_config.validate({'actor_system_config': {'cpu_count': value}})
        recursive = {}
        recursive['cycle'] = recursive
        for value in [recursive, {'x': float('nan')}, {'__proto__': {}}, {'constructor': {}}, {'x': b'bytes'}]:
            with self.assertRaises(BenchmarkError):
                cluster_config.validate(value)
        with self.assertRaises(BenchmarkError):
            cluster_config.parse('metadata: {}\nconfig: {}')
        with self.assertRaises(BenchmarkError):
            cluster_config.parse('actor_system_config: {}\nactor_system_config: {}')

    def test_lists_maps_and_oneof(self):
        def field(name, kind, **kwargs):
            return dict(name=name, type=kind, repeated=False, oneof=None, **kwargs)

        number = field('count', 4)
        schema = {
            'root': 'Root',
            'messages': {
                'Root': [
                    dict(field('items', 11, message='Item'), repeated=True),
                    dict(field('mapping', 11, message='Entry', map=True), repeated=True),
                    dict(field('left', 8), oneof='choice'),
                    dict(field('right', 9), oneof='choice'),
                ],
                'Item': [number],
                'Entry': [field('key', 9), field('value', 11, message='Item')],
            },
        }
        valid = {'items': [{'count': str(2**64 - 1)}], 'mapping': {'a': {'count': 0}}, 'left': False}
        with mock.patch.object(cluster_config, 'schema', return_value=schema):
            self.assertEqual(valid, cluster_config.validate(valid)[0])
            for invalid in [
                dict(valid, right='x'),
                dict(valid, items={}),
                dict(valid, mapping=[]),
                {'items': [{'count': str(2**64)}]},
            ]:
                with self.assertRaises(BenchmarkError):
                    cluster_config.validate(invalid)

    def test_execution_boundary_and_merge(self):
        for value in [{'actor_system_config': {}}, {'blob_storage_config': {}}, {'hosts': []}]:
            with self.assertRaises(BenchmarkError):
                cluster_config.execution_config(value)
        base = {'a': {'b': 1, 'c': [1]}, 'other': True}
        original = copy.deepcopy(base)
        result = cluster_config.merge(base, {'a': {'c': [2]}})
        self.assertEqual({'a': {'b': 1, 'c': [2]}, 'other': True}, result)
        result['a']['c'].append(3)
        self.assertEqual(original, base)

    def test_cluster_settings_and_geometry(self):
        config = {
            'domains_config': {'domain': [{'domain_id': 1, 'name': 'Test'}]},
            'self_management_config': {'erasure_species': 'mirror-3-dc'},
        }
        self.assertEqual(config, cluster_config.execution_config(config))
        self.assertEqual('Test', cluster_config.domain_name(config))
        nodes = [
            {'role': 'static', 'location': {'data_center': str(dc), 'rack': str(rack)}, 'disks': [{'media': 'ssd'}]}
            for dc in range(3)
            for rack in range(3)
        ]
        cluster_config.validate_placement(config, nodes)
        with self.assertRaises(BenchmarkError):
            cluster_config.validate_placement(config, nodes[:-1])
        config['self_management_config']['erasure_species'] = 'block-4-2'
        cluster_config.validate_placement(config, nodes[:-1])
        with self.assertRaises(BenchmarkError):
            cluster_config.validate_placement(config, nodes[:7])
        config['domains_config']['state_storage'] = [{'ssid': 1, 'ring': {'node': [1, 2, 3], 'nto_select': 3}}]
        cluster_config.execution_config(config)
        cluster_config.validate_placement(config, nodes)
        config['domains_config']['state_storage'][0]['ring']['node'] = [100]
        with self.assertRaises(BenchmarkError):
            cluster_config.validate_placement(config, nodes)
        for invalid in [
            {'self_management_config': {'enabled': False}},
            {'domains_config': {'domain': []}},
            {'domains_config': {'domain': [{'name': '../bad'}]}},
            {'domains_config': {'hive_config': []}},
        ]:
            with self.assertRaises(BenchmarkError):
                cluster_config.execution_config(invalid)

    @unittest.skipUnless(shutil.which('node'), 'Node.js is required')
    def test_yaml_leave_confirmation(self):
        script = cluster_config_ui.JS + r'''
const assert=require('assert');
assert.equal(ccItemCaption({name:'SSD'},0),'name · SSD');
assert.equal(ccItemCaption({ssid:0},3),'ssid · 0');
assert.equal(ccItemCaption({nested:{}},2),'Item 3');
const base={flags:{a:true,nested:{b:1}},list:[{x:1}]};
assert.deepStrictEqual(ccInherited(base,[],['flags']),base.flags);
assert.deepStrictEqual(ccInherited(base,[['flags']],['flags']),{});
assert.deepStrictEqual(ccInherited(base,[['flags']],['flags','nested']),{});
assert.deepStrictEqual(ccInherited(base,[['flags','nested']],['flags']),base.flags);
assert.deepStrictEqual(ccInherited(base,[],['list','0']),{});
assert.deepStrictEqual(ccInherited(base,[],['missing']),{});
assert.deepStrictEqual(ccInherited(base,[],[]),base);
const elements=new Map();
const tab={dataset:{ccView:'physical'}};
const app={innerHTML:'',querySelector(selector){
  if(!elements.has(selector))elements.set(selector,{addEventListener(){}});
  return elements.get(selector);
},querySelectorAll(selector){return selector==='[data-cc-view]'?[tab]:[]}};
const shell=(_,content)=>content,esc=value=>value,jsonOptions=value=>value;
const api=async()=>({yaml:'config: {}\n'});
clusterConfigSchema={messages:{}};
let confirmations=0,moves=0;
const confirm=()=>{confirmations++;return false};
(async()=>{
  await renderClusterConfig({name:'QA',tenants:[]},()=>moves++,()=>true,'yaml');
  await tab.onclick();
  assert.equal(moves,1);assert.equal(confirmations,0);
  app.querySelector('#cc-text').oninput({target:{value:'config: {changed: true}\n'}});
  await tab.onclick();
  assert.equal(moves,1);assert.equal(confirmations,1);
  app.querySelector('#cc-text').oninput({target:{value:'config: {}\n'}});
  await tab.onclick();
  assert.equal(moves,2);assert.equal(confirmations,1);
})().catch(error=>{console.error(error);process.exitCode=1});
'''
        subprocess.run(['node', '-e', script], check=True, timeout=10)

    @unittest.skipUnless(shutil.which('node'), 'Node.js is required')
    def test_js_defaults_and_oneof(self):
        subprocess.run(['node', '--check'], input=cluster_config_ui.JS, text=True, check=True, timeout=10)
        script = cluster_config_ui.JS + r'''
const assert=require('assert');
const template={tenants:[{path:'/Root/db'}],nodes:[{tenant:'/Root/db'},{tenant:''}],ydb_tenant_configs:{'/Root/db':{feature_flags:{enable_system_views:false}}}};
ccSetDomain(template,'Demo');assert.equal(ccDomain(template),'Demo');
assert.equal(template.tenants[0].path,'/Demo/db');assert.equal(template.nodes[0].tenant,'/Demo/db');
assert.equal(template.nodes[1].tenant,'');assert.throws(()=>ccSetDomain(template,'bad/name'));
assert.equal(template.ydb_tenant_configs['/Demo/db'].feature_flags.enable_system_views,false);
assert(!Object.hasOwn(template.ydb_tenant_configs,'/Root/db'));
template.ydb_tenant_replacements={'/Demo/db':[['feature_flags']]};
assert.deepStrictEqual(ccMappingPath(template.ydb_tenant_configs['/Demo/db'],template.ydb_tenant_configs['/Demo/db'].feature_flags),['feature_flags']);
assert.equal(ccMappingPath({items:[{}]},{}),null);
ccSetDomain(template,'Next');assert.deepStrictEqual(template.ydb_tenant_replacements['/Next/db'],[['feature_flags']]);
delete template.ydb_tenant_configs['/Next/db'].feature_flags;ccPruneReplacements(template);
assert.deepStrictEqual(template.ydb_tenant_replacements['/Next/db'],[]);
const fields=[{name:'a',type:8,default:false,oneof:'choice'},{name:'b',type:9,default:'',oneof:'choice'}];
const schema={messages:{root:fields}}, value={};
ccAdd(value,fields[0],schema);assert.strictEqual(value.a,false);
assert.throws(()=>ccAdd(value,fields[1],schema),/Remove the current/);
delete value.a;ccAdd(value,fields[1],schema);assert.strictEqual(value.b,'');
assert.deepStrictEqual(ccDefault({repeated:true},schema),[]);
assert.deepStrictEqual(ccDefault({map:true},schema),{});
assert.strictEqual(ccDefault({type:4,default:'18446744073709551615'},schema),'18446744073709551615');
'''
        subprocess.run(['node', '-e', script], check=True, timeout=10)
