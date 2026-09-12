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

    def test_durable_roundtrip_revision_and_delete(self):
        self.assertEqual(self.store.list(), [])
        saved = self.store.save(self.value, {"amd", "sas"})
        self.assertEqual(saved["nodes"][0]["vcpu"], 8)
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
            ("vcpu", True),
            ("role", "unknown"),
            ("sector_map", {"count": 0, "size_gib": 64}),
            ("actor_system", {"use_shared_threads": "false"}),
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
