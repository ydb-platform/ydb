import copy
import json
import shutil
import subprocess
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ydb.tools.ydb_bench.lib import cluster_templates, cluster_templates_ui, topology as topology_module, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.topology import CpuTopology


class ClusterTemplatesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = cluster_templates.ClusterTemplateStore(self.root)
        self.value = {
            "name": "AMD / SAS",
            "nodes": [
                {
                    "name": "storage-1",
                    "role": "static",
                    "host_id": "amd",
                    "binary": "bundled",
                    "vcpu": 8,
                    "sector_map": {"count": 1, "size_gib": 64},
                    "affinity": {"kind": "manual", "cpus": list(range(16))},
                },
                {
                    "name": "compute-1",
                    "role": "dynamic",
                    "host_id": "sas",
                    "binary": "/bin/ydbd",
                    "vcpu": 4,
                    "affinity": {"kind": "strategy", "mode": "pack-numa", "count": 8},
                },
            ],
        }

    def tearDown(self):
        self.temporary.cleanup()

    def test_actor_system_settings_belong_to_runs_not_templates(self):
        self.value["nodes"][0]["actor_system"] = {"use_united_pool": True}
        saved = self.store.save(self.value, {"amd", "sas"})
        for node in saved["nodes"]:
            self.assertNotIn("vcpu", node)
            self.assertNotIn("actor_system", node)
        self.assertEqual(saved["nodes"][1]["affinity"]["count"], 8)
        self.store.save(saved, {"amd", "sas"})

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_view_specific_node_information_and_rack_names(self):
        script = cluster_templates_ui.JS.split("async function renderClusterTemplates")[0] + r"""
const assert=require('assert');
const n={name:'compute',role:'dynamic',host_id:'amd',tenant:'/Root/bench',location:{data_center:'dc',rack:'dc-R1',body:'compute'}};
assert.equal(ctNodeInfo(n,'physical',()=> 'AMD'),'dc / dc-R1 · Tenant: /Root/bench');
assert.equal(ctNodeInfo(n,'logical',()=> 'AMD'),'Host: AMD · Tenant: /Root/bench');
assert.equal(ctNodeInfo(n,'tenants',()=> 'AMD'),'Host: AMD · dc / dc-R1');
assert.equal(ctNodeInfo({...n,role:'cli'},'physical',()=> 'AMD'),'');
assert.equal(ctNextRack({name:'dc',racks:['dc-R1','dc-R3']}),'dc-R2');
assert.equal(ctNextRack({name:'other',racks:[]}),'other-R1');
assert.equal(ctShortHost('amd.example.net',['amd.example.net','sas.example.net']),'amd');
assert.equal(ctShortHost('amd.a.net',['amd.a.net','amd.b.net']),'amd.a.net');
assert.equal(ctDefaultNode('amd','dynamic',1).actor_system,undefined);
"""
        subprocess.run([shutil.which("node"), "-e", script], check=True, capture_output=True, text=True)

    def test_durable_roundtrip_revision_and_delete(self):
        self.assertEqual(self.store.list(), [])
        saved = self.store.save(self.value, {"amd", "sas"})
        self.assertNotIn("vcpu", saved["nodes"][0])
        self.assertEqual(len(saved["nodes"][0]["affinity"]["cpus"]), 16)
        self.assertEqual(cluster_templates.ClusterTemplateStore(self.root).list(), [saved])
        changed = self.store.save(dict(saved, name="Updated"), {"amd", "sas"})
        with self.assertRaises(BenchmarkError):
            self.store.save(saved, {"amd", "sas"})
        with self.assertRaises(BenchmarkError):
            self.store.delete(saved)
        self.assertEqual(self.store.list(), [changed])
        self.store.delete(changed)
        self.assertEqual(self.store.list(), [])

    def test_invalid_nodes_do_not_write(self):
        for field, value in (
            ("host_id", "missing"),
            ("host_id", []),
            ("role", "unknown"),
            ("sector_map", {"count": 0, "size_gib": 64}),
            ("affinity", {"kind": "manual", "cpus": [0, 0]}),
            ("affinity", {"kind": "manual", "cpus": []}),
            ("affinity", {"kind": "strategy", "mode": "unknown", "count": 8}),
        ):
            with self.subTest(field=field, value=value):
                item = copy.deepcopy(self.value)
                item["nodes"][0][field] = value
                with self.assertRaises(BenchmarkError):
                    self.store.save(item, {"amd", "sas"})
        self.assertFalse(self.store.path.exists())

    def test_three_views_roundtrip_and_legacy_migration(self):
        legacy = cluster_templates.validate_template(self.value, {"amd", "sas"})
        self.assertEqual(legacy["host_ids"], ["amd", "sas"])
        self.assertEqual(legacy["data_centers"], [])
        self.assertEqual(legacy["nodes"][0]["tenant"], "")
        legacy["host_ids"].append("empty-host")
        legacy["data_centers"] = [
            {"name": "dc-1", "racks": ["rack-1", "empty-rack"]},
            {"name": "empty-dc", "racks": []},
        ]
        legacy["tenants"] = [{"path": "/Root/orders", "storage_kind": "ssd", "storage_groups": 2}]
        legacy["nodes"][1].update(
            tenant="/Root/orders", location={"data_center": "dc-1", "rack": "rack-1", "body": "server-1"}
        )
        saved = self.store.save(legacy, {"amd", "sas", "empty-host"})
        legacy["nodes"][1]["location"]["body"] = "compute-1"
        for field in ("host_ids", "data_centers", "tenants", "nodes"):
            self.assertEqual(saved[field], legacy[field])
        self.assertEqual(cluster_templates.ClusterTemplateStore(self.root).list(), [saved])

    def test_invalid_placement_references(self):
        for field, value in (
            ("host_ids", ["amd"]),
            ("host_ids", ["amd", "sas", "unknown"]),
            ("host_ids", ["amd", "amd", "sas"]),
            ("data_centers", [{"name": "dc", "racks": ["r", "r"]}]),
            ("data_centers", [{"name": "dc", "racks": []}, {"name": "dc", "racks": []}]),
            ("tenants", [{"path": "/Root/../bad", "storage_kind": "ssd", "storage_groups": 1}]),
            ("tenants", [{"path": "/Root/a", "storage_kind": "ssd", "storage_groups": 0}]),
        ):
            with self.subTest(field=field, value=value):
                with self.assertRaises(BenchmarkError):
                    self.store.save(dict(self.value, **{field: value}), {"amd", "sas"})
        for node_index, updates in (
            (0, {"tenant": "/Root/a"}),
            (1, {"tenant": "/Root/missing"}),
            (1, {"location": {"data_center": "missing"}}),
            (1, {"location": {"data_center": "dc", "rack": "missing"}}),
            (1, {"location": {"body": "server"}}),
        ):
            value = copy.deepcopy(self.value)
            value["data_centers"] = [{"name": "dc", "racks": ["r"]}]
            value["tenants"] = [{"path": "/Root/a", "storage_kind": "ssd", "storage_groups": 1}]
            value["nodes"][node_index].update(updates)
            with self.assertRaises(BenchmarkError):
                self.store.save(value, {"amd", "sas"})
        self.assertFalse(self.store.path.exists())

    def test_cli_has_only_physical_placement(self):
        value = copy.deepcopy(self.value)
        value["data_centers"] = [{"name": "dc", "racks": ["r"]}]
        value["tenants"] = [{"path": "/Root/a", "storage_kind": "ssd", "storage_groups": 1}]
        node = value["nodes"][1]
        node["role"] = "cli"
        saved = cluster_templates.validate_template(value, {"amd", "sas"})
        self.assertEqual(saved["nodes"][1]["tenant"], "")
        self.assertFalse(any(saved["nodes"][1]["location"].values()))
        for update in (
            {"tenant": "/Root/a"},
            {"location": {"data_center": "dc"}},
            {"location": {"data_center": "dc", "rack": "r", "body": "server"}},
        ):
            invalid = copy.deepcopy(value)
            invalid["nodes"][1].update(update)
            with self.assertRaises(BenchmarkError):
                cluster_templates.validate_template(invalid, {"amd", "sas"})

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_moves_change_only_the_selected_view(self):
        script = cluster_templates_ui.JS.split("function ctDefaultNode")[0] + r"""
const assert=require('assert');
const n={name:'compute',host_id:'amd',role:'dynamic',vcpu:8,affinity:{kind:'manual',cpus:[0,1]},tenant:'/Root/a',
  location:{data_center:'dc',rack:'r1',body:'server'}};
const record={nodes:[n],host_ids:['amd','sas'],data_centers:[{name:'dc',racks:['r1','r2']}],tenants:[{path:'/Root/a'},{path:'/Root/b'}]};
const original=JSON.stringify(n);
assert.throws(()=>ctMoveNode(record,0,'physical','sas'));assert.equal(JSON.stringify(n),original);
ctMoveNode(record,0,'logical',['dc','r2']);assert.equal(n.host_id,'amd');assert.equal(n.tenant,'/Root/a');assert.equal(n.location.body,'compute');
ctMoveNode(record,0,'tenants','/Root/b');assert.equal(n.location.rack,'r2');assert.equal(n.host_id,'amd');
ctMoveNode(record,0,'physical','sas',true);assert.equal(n.affinity.mode,'none');assert.equal(n.location.rack,'r2');assert.equal(n.tenant,'/Root/b');
n.affinity={kind:'strategy',mode:'pack-numa',count:8};ctMoveNode(record,0,'physical','amd');assert.equal(n.affinity.mode,'pack-numa');
for(const [view,target] of [['physical','missing'],['logical',['dc','missing']],['tenants','/Root/missing']]){
  const before=JSON.stringify(n);assert.throws(()=>ctMoveNode(record,0,view,target));assert.equal(JSON.stringify(n),before);
}
n.role='static';assert.throws(()=>ctMoveNode(record,0,'tenants','/Root/a'));
n.role='cli';
for(const [view,target] of [['logical',['dc','r1']],['tenants','/Root/a']]){
  const before=JSON.stringify(n);assert.throws(()=>ctMoveNode(record,0,view,target));assert.equal(JSON.stringify(n),before);
}
ctMoveNode(record,0,'physical','sas');assert.equal(n.host_id,'sas');
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)
        subprocess.run(
            [shutil.which("node"), "--check"], input=cluster_templates_ui.JS, text=True, check=True, timeout=10
        )

    def test_default_rack_and_individual_bodies(self):
        value = copy.deepcopy(self.value)
        value["data_centers"] = [{"name": "dc", "racks": []}]
        for node in value["nodes"]:
            node["location"] = {"data_center": "dc"}
        saved = self.store.save(value, {"amd", "sas"})
        self.assertEqual(saved["data_centers"], [{"name": "dc", "racks": ["dc-R1"]}])
        for node in saved["nodes"]:
            self.assertEqual(node["location"], {"data_center": "dc", "rack": "dc-R1", "body": node["name"]})
            node["location"]["body"] = "shared-old-body"
        updated = self.store.save(saved, {"amd", "sas"})
        self.assertEqual([n["location"]["body"] for n in updated["nodes"]], ["storage-1", "compute-1"])

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_removal_preserves_nodes_and_other_placements(self):
        script = cluster_templates_ui.JS.split("async function renderClusterTemplates")[0] + r"""
const assert=require('assert');
const make=()=>({host_ids:['a','b','empty'],data_centers:[{name:'dc',racks:['r1','r2']}],tenants:[{path:'/Root/a'}],nodes:[
  {name:'n1',role:'dynamic',host_id:'a',vcpu:8,affinity:{kind:'manual',cpus:[0]},tenant:'/Root/a',location:{data_center:'dc',rack:'r1',body:'n1'}},
  {name:'n2',role:'static',host_id:'a',affinity:{kind:'strategy',mode:'pack-numa',count:8},tenant:'',location:{data_center:'dc',rack:'r2',body:'n2'}}]});
let r=make();ctRemoveLocation(r,'dc','r1');assert.equal(r.nodes.length,2);assert.equal(r.nodes[0].location.data_center,'');
assert.equal(r.nodes[0].tenant,'/Root/a');assert.equal(r.nodes[0].host_id,'a');assert.equal(r.nodes[1].location.rack,'r2');
assert.deepEqual(r.data_centers[0].racks,['r2']);ctRemoveLocation(r,'dc');assert.deepEqual(r.data_centers,[]);
assert.equal(r.nodes[1].location.data_center,'');
r=make();ctRemoveTenant(r,'/Root/a');assert.equal(r.nodes.length,2);assert.equal(r.nodes[0].tenant,'');
assert.equal(r.nodes[0].location.rack,'r1');assert.equal(r.nodes[0].host_id,'a');assert.deepEqual(r.tenants,[]);
r=make();const before=JSON.stringify(r);assert.throws(()=>ctRemoveHost(r,'a','a'));assert.equal(JSON.stringify(r),before);
assert.throws(()=>ctRemoveHost(r,'a','missing'));assert.equal(JSON.stringify(r),before);
ctRemoveHost(r,'empty');assert.deepEqual(r.host_ids,['a','b']);ctRemoveHost(r,'a','b');assert.deepEqual(r.host_ids,['b']);
assert.equal(r.nodes.length,2);assert(r.nodes.every(n=>n.host_id==='b'));assert.equal(r.nodes[0].affinity.mode,'none');
assert.equal(r.nodes[1].affinity.mode,'pack-numa');assert.equal(r.nodes[0].tenant,'/Root/a');assert.equal(r.nodes[0].location.rack,'r1');
const n=r.nodes[0];n.name='renamed';ctNormalizeNodePlacement(n);assert.equal(n.location.body,'renamed');
assert.throws(()=>ctMoveNode(r,0,'logical',['dc','']));
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_node_rename_rejects_collision_before_mutating_body(self):
        script = cluster_templates_ui.JS.split("async function renderClusterTemplates")[0] + r"""
const assert=require('assert');
const a={name:'a',role:'static',tenant:'',location:{data_center:'dc',rack:'r1',body:'a'}};
const b={name:'b',role:'dynamic',tenant:'/Root/t',location:{data_center:'dc2',rack:'r2',body:'b'}};
const record={nodes:[a,b]};
for(const invalid of ['a',' a ','','   ','x'.repeat(81)]){
  const before=JSON.stringify(record);assert.throws(()=>ctRenameNode(record,b,invalid));assert.equal(JSON.stringify(record),before);
}
ctRenameNode(record,b,' b ');assert.equal(b.name,'b');
ctRenameNode(record,b,' renamed ');assert.equal(b.name,'renamed');assert.equal(b.location.body,'renamed');
assert.equal(b.location.data_center,'dc2');assert.equal(b.location.rack,'r2');assert.equal(b.tenant,'/Root/t');
assert.equal(a.name,'a');assert.equal(a.location.body,'a');
b.role='cli';ctRenameNode(record,b,'load');assert.equal(b.location.body,'');assert.equal(b.tenant,'');
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)

    def test_shared_affinity_roundtrip(self):
        for scope in ("chiplet", "numa"):
            value = copy.deepcopy(self.value)
            value["nodes"][0]["affinity"] = {
                "kind": "strategy",
                "mode": "pack-numa-pack-chiplet",
                "count": 8,
                "scope": scope,
            }
            saved = self.store.save(value, {"amd", "sas"})
            affinity = saved["nodes"][0]["affinity"]
            self.assertNotIn("scope", affinity)
            self.assertEqual(affinity["mode"], "pack-numa" if scope == "numa" else "pack-numa-pack-chiplet")
        for scope in ("bad", [], True):
            with self.assertRaises(BenchmarkError):
                cluster_templates.validate_affinity(
                    {"kind": "strategy", "mode": "pack-numa-pack-chiplet", "count": 8, "scope": scope}
                )

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_shared_ui_reserves_capacity_not_whole_masks(self):
        script = cluster_templates_ui.JS.split("function ctDefaultNode")[0] + r"""
const assert=require('assert');
const node=(name,scope='chiplet')=>({host_id:'a',name,affinity:{kind:'strategy',mode:scope==='numa'?'pack-numa':'pack-numa-pack-chiplet',count:8}});
const all=Array.from({length:32},(_,i)=>i);
const load=async()=>({topology:{allowed_cpus:all,numa_nodes:[{id:0,cpus:all}],
  chiplets:[{numa_node:0,cpus:all.slice(0,16)},{numa_node:0,cpus:all.slice(16)}]}});
(async()=>{
  const a=node('a'),b=node('b'),c=node('c'),d=node('d'),e=node('e');
  let r=await ctResolvePlacements([a,b,c,d,e],load);
  assert.deepEqual(r.get(a).cpus,all.slice(0,16));
  assert.deepEqual(r.get(b).cpus,all.slice(16));
  assert.deepEqual(r.get(c).cpus,r.get(a).cpus);
  assert.equal(new Set([a,b,c,d].flatMap(n=>r.get(n).reserved_cpus)).size,32);
  assert.equal(r.get(e).supported,false);
  const manual={host_id:'a',name:'fixed',affinity:{kind:'manual',cpus:all.slice(0,8)}};
  r=await ctResolvePlacements([a,manual],load);
  assert.deepEqual(r.get(a).cpus,all.slice(16));
  const numa=node('numa','numa');
  r=await ctResolvePlacements([numa,a,manual],load);
  assert.deepEqual(r.get(numa).cpus,all);
  assert(!r.get(numa).reserved_cpus.some(c=>r.get(a).reserved_cpus.includes(c)||manual.affinity.cpus.includes(c)));
  assert.equal(r.get(numa).reserved_cpus.length,8);
  const remote={...a,host_id:'b'};
  r=await ctResolvePlacements([a,remote],load);
  assert.deepEqual(r.get(a).reserved_cpus,r.get(remote).reserved_cpus);
})().catch(e=>{console.error(e);process.exitCode=1});
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)

    def test_overlap_allowed_and_duplicate_names_rejected(self):
        item = copy.deepcopy(self.value)
        item["nodes"].append(dict(item["nodes"][0], name="storage-2"))
        self.store.save(item, {"amd", "sas"})
        item["nodes"][-1]["name"] = "storage-1"
        with self.assertRaises(BenchmarkError):
            self.store.save(item, {"amd", "sas"})

    def test_corrupt_storage_is_not_overwritten(self):
        self.store.path.write_text("not json", encoding="utf-8")
        with self.assertRaises(BenchmarkError):
            self.store.save(self.value, {"amd", "sas"})
        self.assertEqual(self.store.path.read_text(), "not json")

    def test_topology_preview_uses_existing_planner(self):
        topology = CpuTopology(
            tuple(range(4)), ((0, tuple(range(4))),), ((0, tuple(range(4))),), physical_cores=((0, 2), (1, 3))
        )
        service = web.RunService(self.root)
        with mock.patch.object(web, "discover_topology", return_value=topology):
            with mock.patch.object(web, "plan_affinity", wraps=web.plan_affinity) as planner:
                result = service.topology("none", 8)
                self.assertTrue(result["placement"]["supported"])
                self.assertIsNone(result["placement"]["cpus"])
                planner.assert_any_call("none", topology, 8)
            with self.assertRaises(BenchmarkError):
                service.topology("bad", 8)

    def test_record_is_only_a_specification(self):
        saved = self.store.save(self.value, {"amd", "sas"})
        self.assertNotIn("pid", json.dumps(saved))
        self.assertEqual(list(self.root.iterdir()), [self.store.path])

    def test_joint_placement_keeps_chiplets_whole(self):
        topology = CpuTopology(tuple(range(8)), ((0, tuple(range(8))),), ((0, (0, 1, 2, 3)), (0, (4, 5, 6, 7))))
        with mock.patch.object(topology_module.os, "sched_setaffinity", create=True):
            first = topology_module.plan_affinity("pack-numa-pack-chiplet", topology, 1)
            second = topology_module.plan_affinity("pack-numa-pack-chiplet", topology, 1, first.cpus)
            self.assertEqual(first.cpus, (0, 1, 2, 3))
            self.assertEqual(second.cpus, (4, 5, 6, 7))
            self.assertFalse(
                topology_module.plan_affinity("pack-numa-pack-chiplet", topology, 1, tuple(range(8))).supported
            )
            self.assertEqual(
                topology_module.plan_affinity("pack-numa-pack-chiplet", topology, 1, (1,)).cpus, second.cpus
            )

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_joint_ui_allocator_reserves_manual_masks_and_separates_hosts(self):
        script = cluster_templates_ui.JS.split("function ctDefaultNode")[0] + r"""
const assert=require('assert');
const strategy=(host,name)=>({host_id:host,name,affinity:{kind:'strategy',mode:'pack-numa-pack-chiplet-pack-core',count:1}});
const a=strategy('a','first'),b=strategy('a','second'),c=strategy('b','first');
const manual={host_id:'a',name:'manual',affinity:{kind:'manual',cpus:[0,1]}};
const calls=[];
async function load(host,affinity,excluded){
  calls.push({host,excluded:[...excluded]});
  const cpus=[[0,1],[2,3],[4,5]].find(unit=>!unit.some(c=>excluded.includes(c)));
  return {placement:cpus?{supported:true,cpus,excluded_cpus:excluded}:{supported:false,reason:'No free CPUs'}};
}
(async()=>{
  const result=await ctResolvePlacements([a,b,c,manual],load);
  assert.deepEqual(result.get(a).cpus,[2,3]);assert.deepEqual(result.get(b).cpus,[4,5]);
  assert.deepEqual(result.get(c).cpus,[0,1]);assert.deepEqual(manual.affinity.cpus,[0,1]);
  const last=strategy('a','last');
  const exhausted=await ctResolvePlacements([a,b,last,manual],load);
  assert.equal(exhausted.get(last).supported,false);
  const old=await ctResolvePlacements([a,manual],async()=>({placement:{supported:true,cpus:[2,3]}}));
  assert.equal(old.get(a).supported,false);
})().catch(e=>{console.error(e);process.exitCode=1});
"""
        subprocess.check_call([shutil.which("node"), "-e", script], timeout=10)

    def test_http_save_list_preview_and_origin_guard(self):
        server = web.make_server("127.0.0.1", 0, self.root)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = "http://127.0.0.1:{}".format(server.server_address[1])
        try:
            with urlopen(base + "/api/hosts") as response:
                host_id = json.load(response)["local"]["id"]
            value = copy.deepcopy(self.value)
            for node in value["nodes"]:
                node["host_id"] = host_id
            body = json.dumps(value).encode()
            request = Request(base + "/api/cluster-templates", data=body, headers={"Content-Type": "application/json"})
            with urlopen(request) as response:
                saved = json.load(response)
            with urlopen(base + "/api/cluster-templates") as response:
                self.assertEqual(json.load(response), [saved])
            with urlopen(base + "/api/hosts/" + host_id + "/api/system-topology?mode=none&cpus=8") as response:
                self.assertIsNone(json.load(response)["placement"]["cpus"])
            request.add_header("Origin", "http://untrusted.invalid")
            with self.assertRaises(HTTPError) as error:
                urlopen(request)
            self.assertEqual(error.exception.code, 403)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
