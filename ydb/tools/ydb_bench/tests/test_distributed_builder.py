import copy
import json
import shutil
import subprocess
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock
from pathlib import Path

import yaml

from ydb.tools.ydb_bench.lib.config import config_schema, load_config
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

    def load(self, raw=None, benchmark="distributed-ydb"):
        path = self.root / "config.yaml"
        path.write_text(yaml.safe_dump({benchmark: {"test": self.raw if raw is None else raw}}))
        return load_config(path)

    def dedicated(self):
        return copy.deepcopy({key: self.raw[key] for key in ("cluster-template", "storage", "tenants")})

    def test_dedicated_cluster_parser_schema_and_editor(self):
        raw = self.dedicated()
        raw["cluster-template"]["nodes"] = raw["cluster-template"]["nodes"][:2]
        loaded = self.load(raw, "dedicated-ydb")
        configuration = loaded.runs[0]
        self.assertEqual("dedicated-ydb", configuration.benchmark.name)
        self.assertEqual("distributed-ydb", configuration.benchmark.executor)
        self.assertEqual((), configuration.benchmark.metrics)
        parameters = configuration.parameters["local_ydb"]
        self.assertEqual("deploy", parameters["mode"])
        self.assertNotIn("workload", parameters)
        self.assertNotIn("load", parameters)
        self.assertNotIn("cli_nodes", parameters["distributed"])
        self.assertEqual(8, parameters["actor_system"]["static_nodes"]["cpu_count"])
        model = web.editor_model(loaded, self.root)
        self.assertEqual(raw, model["profiles"][0]["distributed_config"])
        self.assertEqual("dedicated-ydb/test", model["profiles"][0]["key"])
        self.assertIn("Dedicated YDB cluster", [b["label"] for b in model["benchmarks"]])
        schema = config_schema()["properties"]["dedicated-ydb"]["additionalProperties"]
        self.assertEqual({"cluster-template", "storage", "tenants", "reset-disks"}, set(schema["properties"]))
        self.assertFalse(schema["additionalProperties"])

    def test_dedicated_cluster_rejects_workload_fields_and_mixed_runs(self):
        raw = self.dedicated()
        for key, value in {"mode": "deploy", "cli-nodes": {}, "measurement": {}, "load": {}, "duration": 5}.items():
            with self.subTest(key=key), self.assertRaisesRegex(BenchmarkError, "unknown fields"):
                self.load({**raw, key: value}, "dedicated-ydb")
        loaded = self.load(raw, "dedicated-ydb")
        with self.assertRaisesRegex(BenchmarkError, "perf is not supported"):
            load_config(loaded.path, perf_enabled=True)
        for other in ({"dedicated-ydb": {"other": raw}}, {"distributed-ydb": {"test": self.raw}}):
            document = {"dedicated-ydb": {"test": raw}}
            for benchmark, profiles in other.items():
                document.setdefault(benchmark, {}).update(profiles)
            loaded.path.write_text(yaml.safe_dump(document))
            with self.assertRaisesRegex(BenchmarkError, "exactly one profile"):
                load_config(loaded.path)

    def test_dedicated_executor_and_saved_profile(self):
        loaded = self.load(self.dedicated(), "dedicated-ydb")
        manifest = {
            "schema_version": 4,
            "state": "completed",
            "runs": [],
            "config": {"snapshot": loaded.path.read_text()},
        }
        run = {
            "root": self.root,
            "loaded": loaded,
            "store": mock.Mock(manifest=manifest),
            "lock": threading.RLock(),
            "finalized": False,
        }
        result = {
            "schema_version": 4,
            "benchmark": "dedicated-ydb",
            "profile": "test",
            "state": "passed",
            "parameters": loaded.runs[0].parameters["local_ydb"],
            "summary": "deployment.txt",
            "attempts": [],
            "endpoints": [{"node": "d", "tenant": "/Root/db", "host": "host", "port": 2135}],
        }

        def deploy(_run, configuration, directory, emit, cancelled):
            self.assertEqual("dedicated-ydb", configuration.benchmark.name)
            (directory / "run.json").write_text(json.dumps(result))
            (directory / "cluster").mkdir()
            (directory / "cluster" / "cluster.yaml").write_text("metadata: {kind: MainConfig}\nconfig: {}\n")
            return result

        with mock.patch.object(web, "run_deployment", side_effect=deploy) as deployment, mock.patch.object(
            web, "run_local_ydb"
        ) as workload, mock.patch.object(web, "load_profile_binaries") as binaries:
            web.production_executor(mock.Mock(), "revision")(run, mock.Mock(), threading.Event())
        deployment.assert_called_once()
        workload.assert_not_called()
        binaries.assert_not_called()
        (self.root / "run.json").write_text(json.dumps(manifest))
        service = mock.Mock(output=self.root)
        with mock.patch.object(web, "_run_directory", return_value=self.root):
            profile = web.RunService.local_ydb_profile(service, "run", "test", "dedicated-ydb")
            saved = web.RunService.run_config(service, "run")
        self.assertEqual(result["endpoints"], profile["endpoints"])
        self.assertEqual([], profile["attempts"])
        self.assertIn("dedicated-ydb/test", saved["profile_yaml"])
        self.assertIn("dedicated-ydb/test", saved["ydb_configurations"])
        self.assertEqual("dedicated-ydb/test/cluster/cluster.yaml", profile["ydb_configurations"][0]["path"])

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_dedicated_builder_views_and_yaml_roundtrip(self):
        raw = self.dedicated()
        model = web.editor_model(self.load(raw, "dedicated-ydb"), self.root)
        script = "const assert=require('assert'),esc=String;const editor={model:" + json.dumps(model) + "};\n"
        script += distributed_builder_ui.JS
        script += web._JS[web._JS.index("function serializeConfig(") : web._JS.index("async function syncEditor(")]
        script += web._JS[web._JS.index("function bindEditorControls(") : web._JS.index("function localField(")]
        script += r"""
const profile=editor.model.profiles[0];
for(const tab of ['Cluster','Storage','Tenants']){
  distributedView.set(profile.key,{tab,item:''});
  const html=distributedProfileEditor(profile);
  assert(html.includes('Dedicated YDB cluster'));
  assert(html.includes('>Type</label>'));
  for(const missing of ['Load generators','Run policy','distributed-mode','Initial target tenant','>c1<','>YAML<'])
    assert(!html.includes(missing),missing);
}
const before=JSON.stringify(profile.distributed_config);
const replaced=distributedReplaceTemplate(profile.distributed_config,rawTemplate(),true).next;
function rawTemplate(){return JSON.parse(JSON.stringify(profile.distributed_config['cluster-template']))}
assert.equal(replaced.storage['cpu-count'],8);assert(!Object.hasOwn(replaced,'mode'));
assert.equal(replaced.tenants['/Root/db']['cpu-count'],16);
assert.equal(JSON.stringify(profile.distributed_config),before);
const storageOnly=JSON.parse(JSON.stringify(profile));
storageOnly.distributed_config['cluster-template'].tenants=[];
const emptyTenants=distributedProfileEditor(storageOnly);
assert(emptyTenants.includes('No tenants in this template.'));assert(!emptyTenants.includes('data-distributed-path'));
let plan='';const elements=new Map(),editorToolbarObserver=null,refreshEditorActivity=()=>{};
const document={querySelector:key=>{
  if(key==='.new-run-page[data-editor-profile]')return null;
  if(!elements.has(key))elements.set(key,{insertAdjacentHTML:(_,html)=>{plan=html}});
  return elements.get(key);
}};
bindEditorControls();
assert(plan.includes('Held until Release cluster'));assert(!plan.includes('load search'));
assert.equal(typeof elements.get('#start-run').onclick,'function');
process.stdout.write(serializeConfig(editor.model));
"""
        serialized = subprocess.check_output([shutil.which("node"), "-e", script], text=True, timeout=10)
        self.assertEqual({"dedicated-ydb": {"test": raw}}, yaml.safe_load(serialized))
        path = self.root / "roundtrip.yaml"
        path.write_text(serialized)
        self.assertEqual("deploy", load_config(path).runs[0].parameters["local_ydb"]["mode"])

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_dedicated_type_switch_without_confirm_validation_and_stale_response(self):
        model = web.editor_model(self.load(), self.root)
        script = "const assert=require('assert'),esc=String;\n" + distributed_builder_ui.JS
        script += "\nconst originalModel=" + json.dumps(model) + ";\n"
        script += r"""
let editorHost='host';const location={hash:'#new'},editor={yaml:'before',perf:true,model:originalModel};
const message={innerHTML:''},select={value:'dedicated-ydb'};
const document={querySelector:key=>key==='#benchmark'?select:message},displayError=e=>e.message;
const jsonOptions=x=>x,serializeConfig=model=>JSON.stringify(model);
const confirm=()=>{throw Error('Type changes must not open native dialogs')};let saved=0,rendered=0;
const saveDraft=()=>saved++,renderNew=()=>rendered++;
let editorApi=async(path,options)=>JSON.parse(options.yaml);
(async()=>{
  const profile=editor.model.profiles[0];
  editorApi=async()=>{throw Error('invalid placement')};
  await chooseDistributedProfile(profile,profile.name,'dedicated-ydb');
  assert.equal(editor.yaml,'before');assert.equal(saved,0);assert(message.innerHTML.includes('invalid placement'));
  assert.equal(select.value,'distributed-ydb');
  let resolve;editorApi=()=>new Promise(done=>{resolve=done});
  const pending=chooseDistributedProfile(profile,profile.name,'dedicated-ydb');
  editorHost='changed';resolve({profiles:[]});await pending;
  assert.equal(editor.yaml,'before');assert.equal(saved,0);
  editorHost='host';editorApi=async(path,options)=>JSON.parse(options.yaml);
  editor.model.profiles.push({...profile,name:'other',key:'distributed-ydb/other'});
  await chooseDistributedProfile(profile,profile.name,'dedicated-ydb');
  assert.equal(editor.model.profiles.length,2);assert.equal(editor.yaml,'before');assert.equal(saved,0);
  assert(message.innerHTML.includes('Remove the other profiles'));
  editor.model.profiles.pop();
  await chooseDistributedProfile(profile,profile.name,'dedicated-ydb');
  assert.equal(editor.model.profiles.length,1);assert.equal(editor.perf,false);
  const dedicated=editor.model.profiles[0];assert.equal(dedicated.benchmark,'dedicated-ydb');
  assert.equal(dedicated.distributed_config.storage['cpu-count'],8);
  assert.equal(dedicated.distributed_config.tenants['/Root/db']['cpu-count'],16);
  for(const key of ['mode','measurement','cli-nodes'])assert(!Object.hasOwn(dedicated.distributed_config,key));
  assert.equal(saved,1);assert.equal(rendered,1);
  await chooseDistributedProfile(dedicated,dedicated.name,'distributed-ydb');
  assert(editor.model.profiles[0].distributed_config['cli-nodes']);assert.equal(saved,2);
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, timeout=10)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_type_switch_from_dedicated_to_standard_builders_without_confirm(self):
        script = "const assert=require('assert');\n" + distributed_builder_ui.JS
        script += r"""
const confirm=()=>{throw Error('Type changes must not open native dialogs')};
const select={},message={innerHTML:''};
const document={querySelector:key=>key==='#benchmark'?select:key==='#distributed-convert'?null:message,
  querySelectorAll:()=>[]};
const editor={};const serializeConfig=model=>JSON.stringify(model);
const saveDraft=()=>{},renderNew=()=>{},defaultLocalYdb=()=>({workload:{type:'kv'}});
bindDistributedTemplate=()=>{};
for(const name of ['ping-bench','memory-bandwidth-bench','local-ydb']){
  const profile={benchmark:'dedicated-ydb',name:'test',key:'dedicated-ydb/test',distributed_config:{}};
  editor.model={profiles:[profile],benchmarks:[{name,profile_kind:name,parameters:[]}]};
  distributedView.set(profile.key,{tab:'Cluster'});
  bindDistributedEditor(profile);
  select.onchange({target:{value:name}});
  assert.equal(editor.model.profiles[0].benchmark,name);
  assert.equal(editor.selected,name+'/test');
  assert(!Object.hasOwn(editor.model.profiles[0],'distributed_config'));
  if(name==='local-ydb')assert.equal(editor.model.profiles[0].local_ydb.workload.type,'kv');
}
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, timeout=10)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_type_switch_back_cancels_pending_validation(self):
        script = "const assert=require('assert');\n" + distributed_builder_ui.JS
        script += r"""
const select={},message={innerHTML:''};
const document={querySelector:key=>key==='#benchmark'?select:key==='#distributed-convert'?null:message,
  querySelectorAll:()=>[]};
const editorHost='host',location={hash:'#new'};
const profile={benchmark:'distributed-ydb',name:'test',key:'distributed-ydb/test',distributed_config:{
  'cluster-template':{tenants:[]},'cli-nodes':{cli:{load:{rate:123}}},measurement:{duration:42}}};
const editor={yaml:'original',perf:true,selected:profile.key,model:{profiles:[profile],benchmarks:
  ['distributed-ydb','dedicated-ydb'].map(name=>({name,profile_kind:name}))}};
const serializeConfig=JSON.stringify,jsonOptions=x=>x,displayError=e=>e.message;
let saved=0,rendered=0;
const saveDraft=()=>saved++,renderNew=()=>rendered++;
bindDistributedTemplate=()=>{};
distributedView.set(profile.key,{tab:'Cluster'});
bindDistributedEditor(profile);
(async()=>{
  for(const fail of [false,true]){
    let settle,pending;
    const before=JSON.stringify(editor);
    editorApi=(path,options)=>{
      assert.equal(path,'/api/editor-config');
      pending=new Promise((resolve,reject)=>{settle=()=>fail?reject(Error('stale error')):resolve(JSON.parse(options.yaml))});
      return pending;
    };
    select.onchange({target:{value:'dedicated-ydb'}});
    assert(settle);
    select.onchange({target:{value:'distributed-ydb'}});
    settle();await pending.catch(()=>{});
    assert.equal(JSON.stringify(editor),before);
    assert.equal(saved,0);assert.equal(rendered,0);assert.equal(message.innerHTML,'');
  }
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, timeout=10)

    def test_per_role_settings_and_independent_clients(self):
        self.raw["cli-nodes"]["c2"]["workload"]["operation"] = "select"
        profile = self.load().runs[0].parameters["local_ydb"]
        self.assertEqual(8, profile["actor_system"]["static_nodes"]["cpu_count"])
        self.assertEqual(16, profile["actor_system"]["tenants"]["/Root/db"]["dynamic_nodes"]["cpu_count"])
        self.assertEqual("select", profile["distributed"]["cli_nodes"]["c2"]["workload"]["operation"])

    def test_deployment_has_no_workload_or_cli_requirement(self):
        raw = {key: self.raw[key] for key in ("cluster-template", "storage", "tenants")}
        raw["mode"] = "deploy"
        profile = self.load(raw).runs[0].parameters["local_ydb"]
        self.assertNotIn("workload", profile)
        self.assertNotIn("load", profile)
        self.assertEqual(["static", "dynamic"], [n["role"] for n in profile["distributed"]["template"]["nodes"]])
        self.assertEqual(8, profile["actor_system"]["static_nodes"]["cpu_count"])
        self.assertEqual(16, profile["actor_system"]["tenants"]["/Root/db"]["dynamic_nodes"]["cpu_count"])
        raw["cluster-template"]["nodes"] = raw["cluster-template"]["nodes"][:1]
        self.load(raw)
        raw["load"] = {"values": [1]}
        with self.assertRaises(BenchmarkError):
            self.load(raw)

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
assert(lines.some(line=>line.trim()==='-'));
assert(!lines.some(line=>line.includes('"nodes": [')));
distributedHosts.set('host','Build host');
const draft=distributedDefault(profile.distributed_config['cluster-template'],'/Root/db');
assert.deepStrictEqual(Object.keys(draft['cli-nodes']),['c1','c2']);
assert.equal(draft['cli-nodes'].c1.workload.options['init-upserts'],1000);
for(const tab of ['Cluster','Storage','Tenants','Load generators','Run policy']){
  distributedView.set(profile.key,{tab,item:''});
  globalThis.localYdbWorkloadDefinition=()=>({options:[],operations:['upsert'],slo_metrics:{p99:'p99_ms'}});
  const html=distributedProfileEditor(profile);assert(!html.includes('>YAML<'));
  assert(!html.includes('id=delete-profile'));
  if(tab==='Cluster'){
    assert(html.includes('Build host'));
    assert(html.includes('<th>Tenant</th>'));assert(html.includes('<th>Affinity</th>'));
    assert(html.includes('dc / dc-R1'));assert(html.includes('/Root/db'));
    assert(html.includes('bundled'));assert(html.includes('No pinning'));
  }
  assert(html.includes('<div class=tabs>'));
  assert(html.includes('class="active" data-distributed-tab="'+tab+'"'));
  assert(!html.includes('class=view-tabs'));
  if(tab==='Storage'||tab==='Tenants'){
    assert(html.includes('class=actor-settings'));
    const actorFlags=html.split('<div class=actor-flags>')[1].split('</div>')[0];
    assert.equal((actorFlags.match(/type=checkbox/g)||[]).length,3);
    for(const flag of ['use-shared-threads','use-united-pool','use-ring-queue'])assert(actorFlags.includes(flag));
    const resetDisks=html.split('<input type=checkbox data-distributed-path="["reset-disks"]" ')[1];
    assert.equal(Boolean(resetDisks),tab==='Storage');
    if(resetDisks)assert(!resetDisks.split('>')[0].includes('checked'));
    assert.equal((html.match(/<select/g)||[]).length,2); // Type and template; actor flags remain checkboxes.
    for(const id of ['benchmark','distributed-template'])assert(html.includes('id='+id));
    assert(!html.includes('distributed-mode'));
  }
  if(tab==='Load generators'){assert(html.includes('Dataset'));assert(html.includes('c1'));assert(html.includes('c2'))}
  if(tab==='Run policy')assert(html.includes('Failed requests remain visible'));
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
    def test_inline_template_preserves_clients_and_defaults_tenants(self):
        model = web.editor_model(self.load(), self.root)
        script = "const assert=require('assert'),esc=String,editor={};\n" + distributed_builder_ui.JS
        script += "\nconst profile=" + json.dumps(model["profiles"][0]) + ";\n"
        script += r"""
const raw=profile.distributed_config,template=raw['cluster-template'];
assert(distributedProfileControls(profile).includes('id=distributed-template'));
assert(distributedProfileControls(profile).includes('id=benchmark'));
assert(!distributedProfileControls(profile).includes('Initial target tenant'));
const automatic=distributedDefault(template);
for(const client of Object.values(automatic['cli-nodes']))assert.equal(client.tenant,'/Root/db');
const changed=JSON.parse(JSON.stringify(template));
changed.nodes=changed.nodes.filter(n=>n.name!=='c2');
changed.nodes.push({...template.nodes.find(n=>n.role==='cli'),name:'c3'});
raw['cli-nodes'].c1.client.threads=17;
raw['cli-nodes'].c1.load['allow-errors']=true;
raw['cli-nodes'].c2.load['allow-errors']=true;
const before=JSON.stringify(raw),replacement=distributedReplaceTemplate(raw,changed);
assert.deepStrictEqual(replacement.removed,['c2']);
assert.equal(replacement.next['cli-nodes'].c1.client.threads,17);
assert.equal(replacement.next['cli-nodes'].c3.tenant,'/Root/db');
assert.equal(replacement.next['cli-nodes'].c3.load['allow-errors'],true);
assert.notEqual(replacement.next['cli-nodes'].c3.dataset,replacement.next['cli-nodes'].c1.dataset);
assert.equal(JSON.stringify(raw),before);
replacement.next['cli-nodes'].c1.client.threads=2;
assert.equal(raw['cli-nodes'].c1.client.threads,17);
const retarget=JSON.parse(JSON.stringify(template));
retarget.tenants[0].path='/Root/new';
for(const node of retarget.nodes)if(node.role==='dynamic')node.tenant='/Root/new';
const moved=distributedReplaceTemplate(raw,retarget);
assert.deepStrictEqual(moved.retargeted,['c1','c2']);
assert.equal(moved.next['cli-nodes'].c1.tenant,'/Root/new');
const empty=JSON.parse(JSON.stringify(template));empty.nodes=empty.nodes.filter(n=>n.role!=='dynamic');
assert.throws(()=>distributedDefault(empty),/tenant with a dynamic node/);
assert.equal(JSON.stringify(profile.distributed_config),before);
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, timeout=10)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_inline_template_cancel_and_stale_directory(self):
        model = web.editor_model(self.load(), self.root)
        script = "const assert=require('assert'),esc=String;\n" + distributed_builder_ui.JS
        script += "\nconst profile=" + json.dumps(model["profiles"][0]) + ";\n"
        script += r"""
let editorHost='host';const editor={yaml:'unchanged'},location={hash:'#new'};
const select={isConnected:true,value:'',innerHTML:'',disabled:true},message={innerHTML:''};
const document={querySelector:s=>s==='#distributed-template'?select:message};
const displayError=e=>e.message;let commits=0;
commitDistributed=async()=>{commits++};
const record=JSON.parse(JSON.stringify(profile.distributed_config['cluster-template']));
record.nodes=record.nodes.filter(n=>n.name!=='c2');
let editorApi=async()=>[record],confirm=()=>false;
(async()=>{
  await bindDistributedTemplate(profile);select.value='0';await select.onchange();
  assert.equal(commits,0);assert.equal(select.value,'');
  confirm=()=>true;select.value='0';await select.onchange();assert.equal(commits,1);
  let release;editorApi=()=>new Promise(resolve=>{release=resolve});
  select.innerHTML='sentinel';const pending=bindDistributedTemplate(profile);
  editorHost='other-host';release([record]);await pending;
  assert.equal(select.innerHTML,'sentinel');assert.equal(editor.yaml,'unchanged');
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, timeout=10)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_common_profile_actions(self):
        script = web._JS[
            web._JS.index("function renameEditorProfile(") : web._JS.index("function rememberEditorDetails(")
        ]
        script = "const assert=require('assert');const editor={};const esc=String;" + distributed_builder_ui.JS + script
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
const dedicated={benchmark:'dedicated-ydb',name:'cluster',key:'dedicated-ydb/cluster',distributed_config:{}};
editor.model={profiles:[dedicated]};
const before=saved;assert.equal(duplicateEditorProfile(dedicated),undefined);assert.equal(saved,before);
assert.equal(editor.model.profiles.length,1);
const html=editorProfileTabs(dedicated);
assert(html.split('id="duplicate-profile"')[1].split('>')[0].includes('disabled'));
renameEditorProfile(dedicated,'renamed-cluster');assert.equal(dedicated.name,'renamed-cluster');
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)
