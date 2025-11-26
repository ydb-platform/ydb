# -*- coding: utf-8 -*-
import os
import json
import requests
import subprocess
import tempfile
import time
import yaml
import copy
from hamcrest import assert_that
import logging

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
import yatest.common as yc


def run_cli(args, input_data=None, allow_nonzero=False):
    bin_env = os.environ.get("YDBD_BINARY")
    bin_path = bin_env if bin_env else yc.binary_path("ydb/apps/ydbd/ydbd")
    cmd = [bin_path] + args
    if os.geteuid() != 0:
        cmd = ["sudo", "-n"] + cmd
    res = subprocess.run(cmd, input=input_data, text=True, capture_output=True)
    if res.returncode != 0 and not allow_nonzero:
        raise RuntimeError("CLI failed (%s): %s\nstdout:\n%s\nstderr:\n%s" % (res.returncode, ' '.join(cmd), res.stdout, res.stderr))
    return res


class TestPDiskMetadata(object):
    metadata_section = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": "",
    }

    def check_all_nodes(self, check_func):
        for node_id, n in self.cluster.nodes.items():
            url = "http://%s:%d/actors/nodewarden" % (n.host, n.mon_port)
            r = requests.get(url, params={"page": "distconf"}, timeout=10)
            body = r.text
            ok = check_func(body)
            if not ok:
                logging.debug("node %s distconf html: %s", node_id, (body[:500] if body else None))
            assert_that(ok)

    @classmethod
    def setup_class(cls):
        cfg = KikimrConfigGenerator(
            Erasure.BLOCK_4_2,
            nodes=8,
            use_in_memory_pdisks=False,
            metadata_section=cls.metadata_section,
            use_self_management=True,
            simple_config=True,
        )
        cls.cluster = KiKiMR(configurator=cfg)
        cls.cluster.start()

        cls.node_to_pdisk_path = {}
        for node_id, n in cls.cluster.nodes.items():
            info = SwaggerClient(n.host, n.mon_port).pdisk_info(node_id)
            pdisks = info.get('PDiskStateInfo') or []
            assert_that(len(pdisks) > 0)
            cls.node_to_pdisk_path[node_id] = pdisks[0]['Path']

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_apply_committed_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = os.path.join(tmp, "committed.yaml")
            yaml_path_mod = os.path.join(tmp, "committed_mod.yaml")
            json_path = os.path.join(tmp, "meta.json")

            for n in self.cluster.nodes.values():
                n.stop()

            pdisk_path_n1 = self.node_to_pdisk_path[self.cluster.nodes[1].node_id]
            res = run_cli(["admin", "blobstorage", "disk", "metadata", "read", pdisk_path_n1,
                           "--committed-yaml", yaml_path, "--json"])
            rec = json.loads(res.stdout)

            committed = rec.get("CommittedStorageConfig", {})
            committed["Generation"] = int(committed.get("Generation", 0)) + 1
            committed["SelfAssemblyUUID"] = "new-cluster-uuid"
            rec["CommittedStorageConfig"] = committed
            with open(json_path, 'w') as f:
                json.dump(rec, f)

            with open(yaml_path, 'r') as f:
                y = yaml.safe_load(f)
            if not isinstance(y, dict):
                y = {}
            meta = y.get('metadata') or {}
            version_before = int(meta.get('version') or 0)
            meta['version'] = version_before + 1
            meta['cluster'] = 'new-cluster'
            y['metadata'] = meta
            with open(yaml_path_mod, 'w') as f:
                yaml.safe_dump(y, f)

            num_nodes = len(self.cluster.nodes)
            majority = num_nodes//2 + 1
            for node_id in self.cluster.nodes:
                majority -= 1
                if not majority:
                    break

                pdisk_path = self.node_to_pdisk_path[node_id]
                run_cli(["admin", "blobstorage", "disk", "metadata", "write", pdisk_path,
                         "--from-json", json_path,
                         "--committed-yaml", yaml_path_mod,
                         "--clear-proposed",
                         "--validate-config"])

            for n in self.cluster.nodes.values():
                n.start()
            time.sleep(10)

            expected_ver = "version: %d" % (version_before + 1)
            expected_cluster = "cluster: new-cluster"
            expected_uuid = "new-cluster-uuid"
            self.check_all_nodes(lambda body: (
                "config.yaml" in body
                and "metadata:" in body
                and (expected_ver in body)
                and (expected_cluster in body)
                and (expected_uuid in body)
            ))

    def test_validate_fails_on_invalid_committed(self):
        for n in self.cluster.nodes.values():
            n.stop()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                json_path = os.path.join(tmp, "meta_invalid.json")
                rec = {
                    "CommittedStorageConfig": {
                        "AllNodes": [
                            {"NodeId": 1, "Host": "localhost", "Port": 70000}
                        ]
                    }
                }
                with open(json_path, 'w') as f:
                    json.dump(rec, f)

                pdisk_path_n1 = self.node_to_pdisk_path[self.cluster.nodes[1].node_id]
                res = run_cli([
                    "admin", "blobstorage", "disk", "metadata", "write", pdisk_path_n1,
                    "--from-json", json_path,
                    "--validate-config",
                ], allow_nonzero=True)
                assert_that(res.returncode != 0)
                assert_that(("validation failed" in res.stdout) or ("validation failed" in res.stderr))
        finally:
            for n in self.cluster.nodes.values():
                n.start()

    def test_write_requires_xor_of_sources(self):
        pdisk_path_n1 = self.node_to_pdisk_path[self.cluster.nodes[1].node_id]
        res = run_cli([
            "admin", "blobstorage", "disk", "metadata", "write", pdisk_path_n1,
            "--from-json", "/nonexistent.json",
            "--from-proto", "/nonexistent.pbtxt",
        ], allow_nonzero=True)
        assert_that(res.returncode != 0)
        assert_that("Specify exactly one of --from-proto or --from-json" in (res.stdout + res.stderr))

    def test_auto_prev_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            yaml_path = os.path.join(tmp, "committed.yaml")
            yaml_path_mod = os.path.join(tmp, "committed_mod.yaml")
            json_path = os.path.join(tmp, "meta.json")

            for n in self.cluster.nodes.values():
                n.stop()

            pdisk_path_n1 = self.node_to_pdisk_path[self.cluster.nodes[1].node_id]

            res = run_cli(["admin", "blobstorage", "disk", "metadata", "read", pdisk_path_n1,
                           "--committed-yaml", yaml_path, "--json"])
            rec = json.loads(res.stdout)

            old_committed = copy.deepcopy(rec.get("CommittedStorageConfig", {}))
            old_generation = old_committed.get("Generation", 0)
            old_committed.pop("PrevConfig", None)

            with open(json_path, 'w') as f:
                json.dump(rec, f)
            with open(yaml_path, 'r') as f:
                y = yaml.safe_load(f)
            if not isinstance(y, dict):
                y = {}
            meta = y.get('metadata') or {}
            version_before = int(meta.get('version') or 0)
            meta['version'] = version_before + 1
            meta['cluster'] = 'auto-prev-cluster'
            y['metadata'] = meta
            with open(yaml_path_mod, 'w') as f:
                yaml.safe_dump(y, f)

            run_cli(["admin", "blobstorage", "disk", "metadata", "write", pdisk_path_n1,
                     "--from-json", json_path,
                     "--committed-yaml", yaml_path_mod,
                     "--auto-prev",
                     "--clear-proposed",
                     "--validate-config"])

            res_after = run_cli(["admin", "blobstorage", "disk", "metadata", "read", pdisk_path_n1,
                                 "--json"])
            rec_after = json.loads(res_after.stdout)

            new_committed = rec_after.get("CommittedStorageConfig", {})
            new_prev = copy.deepcopy(new_committed.get("PrevConfig", {}))
            new_prev.pop("PrevConfig", None)

            assert_that(new_prev)
            assert_that(new_prev.get("Generation") == old_generation)
            assert_that(new_prev == old_committed)
            assert_that("PrevConfig" not in new_prev or not new_prev.get("PrevConfig"))
            for n in self.cluster.nodes.values():
                n.start()
