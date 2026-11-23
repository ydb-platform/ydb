import json
import shlex
from pathlib import Path
from subprocess import CompletedProcess, run
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ydb.tools.dc_admin import static_recovery


class TestStaticRecovery(TestCase):
    def test_resume_partial_write(self):
        self.check_recovery()

    def test_resume_partial_write_at_inv2(self):
        self.check_recovery(group_state="DC_FAILURE_INV2")

    def test_unconfirmed_state_stops_before_writing_and_restarts_nodes(self):
        self.check_recovery(group_state="DC_FAILURE_INV2_PENDING", rejected=True)

    def test_state_storage_two_active_datacenters(self):
        self.check_recovery(["dc1", "dc2"])

    def check_recovery(self, state_dcs=None, group_state="DC_FAILURE_INV1", rejected=False):
        inventory = {}
        nodes, disks, rings = [], [], []
        records, states = {}, {}
        for dc in range(1, 4):
            domains = []
            for node in range(1, 4):
                unit = f"ydb_dc{dc}_node{node}.service"
                inventory[unit] = {"dc": f"dc{dc}", "node": node}
                states[unit] = "active" if (dc == 1 or state_dcs and dc == 2 and node == 1) else "inactive"
                path = f"/dev/mapper/ydb_dc{dc}_node{node}_pdisk"
                location = {"NodeID": dc * 10 + node, "PDiskID": 1}
                disks.append({**location, "Path": path})
                nodes.append({"NodeId": location["NodeID"], "Location": {"DataCenter": f"dc{dc}"}})
                domains.append({"VDiskLocations": [location]})
                records[path] = None
            rings.append({"State": "EXCLUDED" if dc == 3 else "INCLUDED", "FailDomains": domains})
        initial_states = states.copy()
        group = {"GroupID": 0, "Mirror3dcState": group_state, "Rings": rings}
        source = {
            "CommittedStorageConfig": {
                "AllNodes": nodes,
                "SelfAssemblyUUID": "test",
                "Generation": "5",
                "BlobStorageConfig": {"ServiceSet": {"Groups": [group], "PDisks": disks}},
            }
        }
        records = dict.fromkeys(records, source)
        target = {"CommittedStorageConfig": {"Generation": "6"}}
        writes = []

        progress = []

        def execute(args, **kwargs):
            expected = shlex.join(args)
            if args[0] != "systemctl":
                self.assertTrue(progress[-1].splitlines()[-1].startswith("$ " + expected))
            output = ""
            if args[0] in ("cat", "diff"):
                return run(args, **kwargs)
            if args[0] == "systemctl":
                output = states[args[-1]]
            elif args[0] == "sudo":
                for unit in args[4:]:
                    states[unit] = "active" if args[3] == "start" else "inactive"
            elif Path(args[0]).name == "dc-admin-prepare-solo":
                self.assertEqual(args[1], "state-storage" if state_dcs else "group")
                self.assertEqual(args[3:], state_dcs or ["dc2", "dc1"])
                output = json.dumps(target)
            elif args[5] == "read":
                self.assertTrue(any(dc in args[-1] for dc in state_dcs or ["dc1"]))
                output = json.dumps(records[args[-1]])
            else:
                self.assertEqual(args[5], "write")
                writes.append(args[-1])
                if len(writes) == 2:
                    return CompletedProcess(args, 1, "", "write failed")
                records[args[-1]] = json.loads(Path(args[-2]).read_text())
            return CompletedProcess(args, 0, output, "")

        with TemporaryDirectory() as tmp, patch.object(Path, "is_block_device", return_value=True), patch.object(
            static_recovery.subprocess, "run", side_effect=execute
        ):
            archive = Path(tmp)
            if rejected:
                with self.assertRaisesRegex(ValueError, "подтверждённом"):
                    static_recovery.run_recovery("dc1", inventory, "ydbd", archive, progress=progress.append)
                self.assertFalse(writes)
                self.assertEqual(states, initial_states)
                self.assertFalse((archive / "pending").exists())
                return
            path = "/dev/mapper/ydb_dc1_node3_pdisk"
            records[path] = {"CommittedStorageConfig": {**source["CommittedStorageConfig"], "Generation": "4"}}
            with self.assertRaisesRegex(ValueError, "diff -q"):
                static_recovery.run_recovery(
                    "dc1", inventory, "ydbd", archive, progress=progress.append, state_dcs=state_dcs
                )
            self.assertFalse(writes)
            self.assertFalse((archive / "pending").exists())
            records[path] = source
            with self.assertRaisesRegex(ValueError, "write failed"):
                static_recovery.run_recovery(
                    "dc1", inventory, "ydbd", archive, progress=progress.append, state_dcs=state_dcs
                )
            self.assertEqual(set(states.values()), {"inactive"})
            pending = archive / "pending"
            self.assertTrue(pending.exists())
            diff = (pending / "config.diff").read_text()
            self.assertIn('-  "Generation": "5"', diff)
            self.assertIn('+  "Generation": "6"', diff)
            self.assertNotIn("systemctl show", progress[-1])
            self.assertEqual(progress[-1].count("$ diff -q "), 4 if state_dcs else 2)
            self.assertEqual(progress[-1].count(diff.rstrip()), 1)
            self.assertEqual(json.loads((pending / "target.json").read_text()), target)
            static_recovery.run_recovery(
                "dc1", inventory, "ydbd", archive, progress=progress.append, state_dcs=state_dcs
            )
            self.assertNotIn(diff.rstrip(), progress[-1])
            self.assertFalse((archive / "pending").exists())
            self.assertEqual(writes.count(writes[0]), 1)
            for path, record in records.items():
                self.assertEqual(record, target if any(dc in path for dc in state_dcs or ["dc1"]) else source)
            for unit, state in states.items():
                self.assertEqual(state, initial_states[unit])
