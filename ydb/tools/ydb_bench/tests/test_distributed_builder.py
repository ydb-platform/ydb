import copy
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

from ydb.tools.ydb_bench.lib.config import load_config
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib import distributed_builder_ui, web


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
        with self.assertRaisesRegex(BenchmarkError, "identical KV options"):
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
  globalThis.localYdbWorkloadDefinition=()=>({options:[]});
  const html=distributedProfileEditor(profile);assert(!html.includes('>YAML<'));
  if(tab==='Load generators'){assert(html.includes('Dataset'));assert(html.includes('c1'));assert(html.includes('c2'))}
}
process.stdout.write('distributed-ydb:\\n  test:\\n'+lines.join('\\n'));
"""
        result = subprocess.check_output([shutil.which("node"), "-e", script], text=True, timeout=10)
        self.assertEqual({"distributed-ydb": {"test": self.raw}}, yaml.safe_load(result))
