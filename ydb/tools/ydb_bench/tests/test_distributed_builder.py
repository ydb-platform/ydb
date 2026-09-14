import copy
import json
import shutil
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock
from pathlib import Path

import yaml

from ydb.tools.ydb_bench.lib.config import load_config
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib import distributed_builder_ui, distributed_runtime, distributed_workload, web


class DistributedBuilderTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        nodes = []
        for name, role in (("s", "static"), ("d", "dynamic"), ("c1", "cli"), ("c2", "cli")):
            nodes.append(
                {
                    "name": name,
                    "role": role,
                    "host_id": "host",
                    "binary": "bundled",
                    "affinity": {"kind": "strategy", "mode": "none", "count": 1},
                    "location": {} if role == "cli" else {"data_center": "dc", "rack": "dc-R1"},
                    "tenant": "/Root/db" if role == "dynamic" else "",
                    **({"sector_map": {"count": 1, "size_gib": 1}} if role == "static" else {}),
                }
            )
        self.raw = {
            "cluster-template": {
                "name": "test",
                "host_ids": ["host"],
                "nodes": nodes,
                "data_centers": [{"name": "dc", "racks": ["dc-R1"]}],
                "tenants": [{"path": "/Root/db", "storage_kind": "ssd", "storage_groups": 1}],
            },
            "storage": {"cpu-count": 8},
            "tenants": {"/Root/db": {"cpu-count": 16, "use-united-pool": True}},
            "cli-nodes": {
                name: {
                    "tenant": "/Root/db",
                    "dataset": "shared",
                    "workload": {"type": "kv", "operation": "upsert", "options": {"init-upserts": 1000}},
                    "client": {"threads": 4},
                    "load": {"parameter": "threads", "values": [4]},
                }
                for name in ("c1", "c2")
            },
            "measurement": {"duration": 1, "warmup": 0, "repetitions": 1, "verification-repetitions": 0},
        }

    def load(self, raw=None):
        path = self.root / "config.yaml"
        path.write_text(yaml.safe_dump({"distributed-ydb": {"test": self.raw if raw is None else raw}}))
        return load_config(path)

    def test_per_role_settings_and_independent_clients(self):
        self.raw["cli-nodes"]["c2"]["workload"]["operation"] = "select"
        profile = self.load().runs[0].parameters["local_ydb"]
        self.assertEqual(8, profile["actor_system"]["static_nodes"]["cpu_count"])
        self.assertEqual(16, profile["actor_system"]["tenants"]["/Root/db"]["dynamic_nodes"]["cpu_count"])
        self.assertEqual("select", profile["distributed"]["cli_nodes"]["c2"]["workload"]["operation"])

    def test_shared_dataset_rejects_incompatible_options(self):
        self.raw["cli-nodes"]["c2"]["workload"]["options"]["columns"] = 3
        with self.assertRaisesRegex(BenchmarkError, "identical workload type and options"):
            self.load()
        self.raw["cli-nodes"]["c2"]["dataset"] = "independent"
        self.load()

    def test_missing_client_and_invalid_dataset(self):
        missing = copy.deepcopy(self.raw)
        del missing["cli-nodes"]["c2"]
        with self.assertRaisesRegex(BenchmarkError, "every CLI"):
            self.load(missing)
        self.raw["cli-nodes"]["c1"]["dataset"] = "../outside"
        with self.assertRaisesRegex(BenchmarkError, "dataset"):
            self.load()

    def test_only_one_cli_may_own_search(self):
        for client in self.raw["cli-nodes"].values():
            client["load"] = {"search": {"start": 1, "maximum": 10}}
        with self.assertRaisesRegex(BenchmarkError, "at most one CLI"):
            self.load()

    def search(self):
        self.raw['cli-nodes']['c2']['load'] = {
            'parameter': 'threads',
            'search': {'start': 2, 'maximum': 32},
            'objective': {'type': 'latency-slo', 'percentile': 'p99', 'max-ms': 20},
        }

    def test_stock_and_search_select_the_second_cli(self):
        client = self.raw['cli-nodes']['c2']
        client['dataset'] = 'stock'
        client['workload'] = {'type': 'stock', 'operation': 'put-rand-order'}
        self.search()
        self.raw['measurement']['verification-repetitions'] = 2
        profile = self.load().runs[0].parameters['local_ydb']
        self.assertEqual('c2', profile['distributed']['search_cli'])
        self.assertEqual('stock', profile['workload']['type'])
        self.assertEqual(32, profile['load']['search']['maximum'])
        self.assertEqual([4], profile['distributed']['cli_nodes']['c1']['load']['values'])
        self.assertEqual(2, profile['measurement']['verification_repetitions'])

    def test_stock_table_names_prevent_independent_datasets_in_one_tenant(self):
        for name, client in self.raw['cli-nodes'].items():
            client['workload'] = {'type': 'stock', 'operation': 'put-rand-order'}
            client['dataset'] = name
        with self.assertRaisesRegex(BenchmarkError, 'fixed table names'):
            self.load()
        self.raw['cli-nodes']['c2']['dataset'] = 'c1'
        self.load()

    def test_worker_changes_only_search_cli_load(self):
        self.search()
        profile = self.load().runs[0].parameters['local_ydb']
        worker = distributed_workload.MultiWorkerWorkload.__new__(distributed_workload.MultiWorkerWorkload)
        worker.single = None
        worker.clients = profile['distributed']['cli_nodes']
        worker.workloads = {
            name: SimpleNamespace(perform=mock.Mock(return_value={'artifacts': []})) for name in worker.clients
        }
        worker.perform('sample', {'load': 17})
        self.assertEqual(4, worker.workloads['c1'].perform.call_args.args[1]['load'])
        self.assertEqual(17, worker.workloads['c2'].perform.call_args.args[1]['load'])

    def test_search_uses_selected_cli_metrics_not_aggregate(self):
        self.search()
        configuration = self.load().runs[0]
        clients = {
            name: {'metrics': {'throughput': value, 'p99_ms': value}, 'commands': [], 'artifacts': []}
            for name, value in [('c1', 999), ('c2', 7)]
        }
        lifecycle = distributed_runtime.RemoteWorkloadLifecycle.__new__(distributed_runtime.RemoteWorkloadLifecycle)
        lifecycle.configuration = configuration
        lifecycle.output = self.root
        lifecycle.sequence = 0
        lifecycle.multiple = True
        lifecycle.cluster = SimpleNamespace(
            cli_hosts=['host'], reference={}, operation=mock.Mock(return_value={'host': {'clients': clients}})
        )
        with mock.patch.object(distributed_runtime, 'copy_results'):
            result = lifecycle._perform('sample', {'directory': self.root, 'load': 17})
        self.assertEqual(clients['c2'], result)
        self.assertEqual(set(clients), set(json.loads((self.root / 'cli-results.json').read_text())))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_builder_yaml_and_views(self):
        loaded = self.load()
        model = web.editor_model(loaded, self.root)
        script = (
            "const assert=require('assert');const esc=v=>String(v??'');const editor={};\n" + distributed_builder_ui.JS
        )
        script += "\nconst profile=" + json.dumps(model["profiles"][0]) + ";\n"
        script += """
const lines=[];serializeDistributedYdb(lines,profile);
const draft=distributedDefault(profile.distributed_config['cluster-template'],'/Root/db');
assert.deepStrictEqual(Object.keys(draft['cli-nodes']),['c1','c2']);
assert.equal(draft['cli-nodes'].c1.workload.options['init-upserts'],1000);
for(const tab of ['Cluster','Storage','Tenants','Load generators','Run policy']){
  distributedView.set(profile.key,{tab,item:''});
  globalThis.localYdbWorkloadDefinition=()=>({options:[],operations:['upsert'],slo_metrics:{p99:'p99_ms'}});
  const html=distributedProfileEditor(profile);assert(!html.includes('>YAML<'));
  assert(!html.includes('id=delete-profile'));
  if(tab==='Cluster'){
    assert(html.includes('<th>Tenant</th>'));assert(html.includes('<th>Affinity</th>'));
    assert(html.includes('dc / dc-R1'));assert(html.includes('/Root/db'));
    assert(html.includes('bundled'));assert(html.includes('No pinning'));
  }
  assert(html.includes('<div class=tabs>'));
  assert(html.includes('class="active" data-distributed-tab="'+tab+'"'));
  assert(!html.includes('class=view-tabs'));
  if(tab==='Storage'||tab==='Tenants'){
    assert.equal((html.match(/type=checkbox/g)||[]).length,3);
    assert(!html.includes('<select'));
  }
  if(tab==='Load generators'){assert(html.includes('Dataset'));assert(html.includes('c1'));assert(html.includes('c2'))}
}
distributedSetLoadMode(draft,'c2','latency-slo');
assert.equal(draft['cli-nodes'].c2.load.objective.type,'latency-slo');
assert.throws(()=>distributedSetLoadMode(draft,'c1','maximize-throughput'),/Only one CLI/);
assert.deepStrictEqual(draft['cli-nodes'].c1.load.values,[1]);
const searchProfile={...profile,distributed_config:draft};
distributedView.set(profile.key,{tab:'Load generators',item:'c2'});
assert(distributedProfileEditor(searchProfile).includes('Maximum latency (ms)'));
distributedSetLoadMode(draft,'c2','fixed');
assert(!draft['cli-nodes'].c2.load.search);
const legacy={...profile,distributed_config:{}};
assert(!distributedProfileEditor(legacy).includes('id=delete-profile'));
process.stdout.write('distributed-ydb:\\n  test:\\n'+lines.join('\\n'));
"""
        result = subprocess.check_output([shutil.which("node"), "-e", script], text=True, timeout=10)
        self.assertEqual({"distributed-ydb": {"test": self.raw}}, yaml.safe_load(result))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_common_profile_actions(self):
        script = web._JS[
            web._JS.index("function renameEditorProfile(") : web._JS.index("function rememberEditorDetails(")
        ]
        script = (
            "const assert=require('assert');const editor={};const distributedView=new Map();const esc=String;" + script
        )
        script += """
globalThis.serializeConfig=model=>JSON.stringify(model.profiles);
let saved=0;globalThis.saveDraft=()=>saved++;
for(const benchmark of ['local-ydb','distributed-ydb','ping-bench']){
  const p={benchmark,name:'main',key:benchmark+'/main',config:{load:{values:[1,2]}}};
  editor.model={profiles:[p]};distributedView.set(p.key,{tab:'Storage'});
  assert(editorProfileTabs(p).includes('disabled'));
  renameEditorProfile(p,'renamed');assert.equal(p.key,benchmark+'/renamed');
  assert.equal(editor.selected,p.key);assert(distributedView.has(p.key));assert(!distributedView.has(benchmark+'/main'));
  assert.throws(()=>renameEditorProfile(p,'bad/name'));
  const copy=duplicateEditorProfile(p);assert.equal(copy.name,'renamed-copy');
  copy.config.load.values.push(3);assert.deepStrictEqual(p.config.load.values,[1,2]);
  assert.throws(()=>renameEditorProfile(copy,'renamed'));
  const second=duplicateEditorProfile(p);assert.equal(second.name,'renamed-copy-1');
  const html=editorProfileTabs(copy);assert.equal((html.match(/id="delete-profile"/g)||[]).length,1);
  removeEditorProfile(second);removeEditorProfile(copy);removeEditorProfile(p);
  assert.deepStrictEqual(editor.model.profiles,[p]);assert.equal(editor.selected,p.key);
  assert.deepStrictEqual(JSON.parse(editor.yaml),[p]);
}
assert(saved>0);
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)
