import errno
import http.client
import json
import shlex
import socket
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest.mock import patch

from ydb.tools.dc_admin import server, storage


class TestDataCenterAdmin(unittest.TestCase):
    endpoint = "grpc://ydb-v6.example.test:2135"

    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        journal_env = patch.dict("os.environ", DC_ADMIN_JOURNAL=str(Path(directory.name) / "commands.db"))
        journal_env.start()
        self.addCleanup(journal_env.stop)
        self.cli = Path(directory.name) / "ydb"
        self.cli.write_text(
            '#!/bin/sh\n'
            'printf "%s\\n" "$@" > "$0.args"\n'
            'if [ -f "$0.fail" ]; then echo "permission denied" >&2; exit 1; fi\n'
            'while [ -f "$0.wait" ]; do sleep 0.05; done\n'
        )
        self.cli.chmod(0o755)
        self.args = self.cli.with_suffix(".args")

    def start_server(self, database="/Root", datacenters=("zone-a", "zone-b"), require_ipv6=False, monitoring_url=None):
        listen = {}
        try:
            with socket.socket(socket.AF_INET6) as probe:
                probe.bind(("::1", 0))
        except OSError as exc:
            if exc.errno not in (
                errno.EAFNOSUPPORT,
                errno.EPROTONOSUPPORT,
                errno.EADDRNOTAVAIL,
                errno.EACCES,
                errno.EPERM,
            ):
                raise
            if require_ipv6:
                self.skipTest(f"IPv6 loopback is unavailable: {exc}")
            listen["listen"] = "127.0.0.1"
        httpd = server.create_server(
            endpoint=self.endpoint,
            database=database,
            datacenters=list(datacenters),
            ydb_binary=str(self.cli),
            monitoring_url=monitoring_url,
            port=0,
            **listen,
        )
        thread = Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(httpd.shutdown)
        self.address = httpd.server_address[:2]
        status, config = self.request("GET", "/api/config")
        self.assertEqual(status, 200)
        return config

    def request(self, method, path, payload=None, headers=None):
        connection = http.client.HTTPConnection(*self.address, timeout=5)
        try:
            body = None if payload is None else json.dumps(payload)
            connection.request(method, path, body, headers or {})
            response = connection.getresponse()
            data = response.read()
            if response.getheader("Content-Type", "").startswith("application/json"):
                data = json.loads(data)
            return response.status, data
        finally:
            connection.close()

    def action(self, action="takedown", datacenter="zone-a", dry_run=False, **headers):
        return self.request(
            "POST",
            "/api/action",
            {"action": action, "datacenter": datacenter, "dryRun": dry_run},
            {"Content-Type": "application/json", **headers},
        )

    def test_serves_page_and_cluster_config(self):
        config = self.start_server()
        self.assertEqual(config.pop("workloadDatabase"), "/Root")
        self.assertEqual(config.pop("workloadEndpoint"), self.endpoint)
        self.assertTrue(config.pop("command").endswith("-y admin storage datacenter"))
        self.assertEqual(config, {"endpoint": self.endpoint, "database": "/Root", "datacenters": ["zone-a", "zone-b"]})
        status, page = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"<html", page.lower())
        self.assertFalse(self.args.exists())

    def test_ipv6_loopback_takedown_dry_run(self):
        self.start_server(require_ipv6=True)
        host, port = self.address
        self.assertEqual(host, "::1")
        status, result = self.action(dry_run=True, Origin=f"http://[{host}]:{port}")
        self.assertEqual(status, 200, result)
        self.assertEqual(self.args.read_text().splitlines()[:2], ["-e", self.endpoint])

    def test_host_controls_only_configured_node_services(self):
        unit = "ydb_zone-a_node1.service"
        with patch.object(Path, "glob", return_value=[Path(unit), Path("ydb_zone-c_node2.service")]):
            self.start_server()
        headers = {"Content-Type": "application/json"}
        with patch.object(server.subprocess, "run") as run:
            run.return_value.stdout = f"ActiveState=inactive\nId={unit}\n"
            status, data = self.request("GET", "/api/hosts")
            self.assertEqual(status, 200)
            self.assertEqual(data["hosts"], [{"dc": "zone-a", "node": 1, "unit": unit, "state": "inactive"}])
            run.reset_mock()
            for invalid in ("ssh.service", "ydb_zone-c_node2.service"):
                status, _ = self.request("POST", "/api/host", {"unit": invalid, "action": "stop"}, headers)
                self.assertEqual(status, 400)
            run.assert_not_called()
            run.return_value.stdout = ""
            run.return_value.returncode = 0
            with patch.object(server, "run_command", run):
                for action in ("start", "stop"):
                    status, result = self.request("POST", "/api/host", {"unit": unit, "action": action}, headers)
                    self.assertEqual(status, 200)
                    self.assertEqual(shlex.split(result["command"]), ["sudo", "-n", "systemctl", action, unit])
                    self.assertEqual(run.call_args.args[0], shlex.split(result["command"]))

    def test_static_recovery_uses_server_node_manifest(self):
        unit = "ydb_zone-a_node1.service"
        manifest = self.cli.parent / "nodes.json"
        nodes = {
            "1": {
                "unit": unit.removesuffix(".service"),
                "dc": "zone-a",
                "pdisk": "/dev/test-pdisk",
                "mon": 1234,
                "grpc": 0,
            }
        }
        manifest.write_text(json.dumps(nodes))
        env = {"DC_ADMIN_NODES": str(manifest), "DC_ADMIN_STATIC_RECOVERY_DIR": str(self.cli.parent / "archive")}
        with patch.dict(server.os.environ, env), patch.object(Path, "glob", return_value=[Path(unit)]):
            self.start_server()
        headers = {"Content-Type": "application/json"}
        with patch.object(server, "run_recovery", return_value={"output": "", "exitCode": 0}) as recover:
            status, _ = self.request(
                "POST", "/api/static-recovery", {"datacenter": "zone-a", "pdisk": "/dev/other"}, headers
            )
            self.assertEqual(status, 400)
            recover.assert_not_called()
            status, _ = self.request("POST", "/api/static-recovery", {"datacenter": "zone-a"}, headers)
            self.assertEqual(status, 200)
            self.assertEqual(recover.call_args.args[1][unit]["pdisk"], "/dev/test-pdisk")

    def test_host_action_targets_all_units_in_one_dc(self):
        units = [Path(f"ydb_zone-{dc}_node{node}.service") for dc in ("a", "b") for node in (1, 2)]
        with patch.object(Path, "glob", return_value=units):
            self.start_server()
        headers = {"Content-Type": "application/json"}
        with patch.object(server, "run_command") as run:
            for params in ({"datacenter": "missing"}, {"datacenter": "zone-a", "unit": units[0].name}):
                self.assertEqual(self.request("POST", "/api/host", {"action": "stop", **params}, headers)[0], 400)
            run.assert_not_called()
            run.return_value.stdout = ""
            run.return_value.returncode = 0
            for action in ("stop", "start"):
                status, result = self.request("POST", "/api/host", {"action": action, "datacenter": "zone-a"}, headers)
                self.assertEqual(status, 200)
                argv = ["sudo", "-n", "systemctl", action, units[0].name, units[1].name]
                self.assertEqual(run.call_args.args[0], argv)
                self.assertEqual(shlex.split(result["command"]), argv)

    def test_runs_each_cli_action(self):
        self.start_server()
        for action, dry_run in (("takedown", True), ("takedown", False), ("rejoin", False)):
            with self.subTest(action=action, dry_run=dry_run):
                status, result = self.action(action, dry_run=dry_run)
                self.assertEqual(status, 200, result)
                self.assertIn("output", result)
                self.assertEqual(result["exitCode"], 0)
                self.assertEqual(shlex.split(result["command"]), [str(self.cli), *self.args.read_text().splitlines()])
                self.assertEqual(
                    self.args.read_text().splitlines(),
                    ["-e", self.endpoint, "-d", "/Root", "-y", "admin", "storage", "datacenter", action]
                    + ["--datacenter-name=zone-a"]
                    + (["--dry-run"] if dry_run else []),
                )

    def test_accepts_free_name_without_database(self):
        self.start_server(database="", datacenters=())
        status, result = self.action("rejoin", "zone-c")
        self.assertEqual(status, 200, result)
        args = self.args.read_text().splitlines()
        self.assertNotIn("-d", args)
        self.assertIn("--datacenter-name=zone-c", args)

    def test_rejects_invalid_action_parameters(self):
        self.start_server()
        for action, dc, dry_run in (
            ("rejoin", "zone-a", True),
            ("takedown", "zone-c", False),
            ("takedown", "", False),
            ("replace", "zone-a", False),
            ("takedown", "zone-a", "false"),
        ):
            with self.subTest(action=action, dc=dc, dry_run=dry_run):
                status, _ = self.action(action, dc, dry_run)
                self.assertEqual(status, 400)
                self.assertFalse(self.args.exists())

    def test_rejects_unknown_host_or_cross_origin(self):
        self.start_server()
        self.assertEqual(self.request("GET", "/api/config", headers={"Host": "unknown.example"})[0], 403)
        status, _ = self.action(Origin="http://another-host")
        self.assertEqual(status, 403)
        self.assertFalse(self.args.exists())

    def test_cli_failure_is_reported_and_next_command_can_run(self):
        self.start_server()
        failure = self.cli.with_suffix(".fail")
        failure.touch()
        status, result = self.action()
        self.assertEqual(status, 502)
        self.assertIn("permission denied", result["error"])
        self.assertEqual(result["output"], "permission denied\n")
        self.assertEqual(result["exitCode"], 1)
        failure.unlink()
        self.assertEqual(self.action("rejoin")[0], 200)

    def test_only_one_action_at_a_time(self):
        self.start_server()
        hold = self.cli.with_suffix(".wait")
        hold.touch()
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(self.action)
            try:
                deadline = time.monotonic() + 3
                while not self.args.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(self.args.exists(), "first request never reached the CLI")
                self.assertEqual(self.action("rejoin", "zone-b")[0], 409)
            finally:
                hold.unlink()
            self.assertEqual(first.result()[0], 200)

    def test_cancel_running_command_by_id(self):
        self.start_server()
        hold = self.cli.with_suffix(".wait")
        hold.touch()
        headers = {"Content-Type": "application/json"}
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(self.action)
            try:
                deadline = time.monotonic() + 3
                while not self.args.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                (row,) = self.request("GET", "/api/commands")[1]["commands"]
                self.assertEqual(row["state"], "running")
                self.assertEqual(self.request("POST", "/api/commands/cancel", {"id": row["id"]}, headers)[0], 202)
                self.assertEqual(first.result(timeout=3)[0], 502)
                (row,) = self.request("GET", "/api/commands")[1]["commands"]
                self.assertEqual(row["state"], "cancelled")
                self.assertEqual(self.request("POST", "/api/commands/cancel", {"id": row["id"]}, headers)[0], 409)
            finally:
                hold.unlink()

    def test_workload_rejects_cross_origin(self):
        self.start_server()
        for path in ("/api/workload/start", "/api/workload/stop"):
            with self.subTest(path=path):
                headers = {"Content-Type": "application/json", "Origin": "https://other.example"}
                self.assertEqual(self.request("POST", path, {}, headers)[0], 403)
        self.assertFalse(self.args.exists())

    def test_workload_parameters_are_server_controlled(self):
        self.start_server()
        headers = {"Content-Type": "application/json"}
        for path in ("/api/workload/start", "/api/workload/stop"):
            for body in (None, [], {"threads": 128}):
                with self.subTest(path=path, body=body):
                    self.assertEqual(self.request("POST", path, body, headers)[0], 400)
            self.assertEqual(self.request("POST", path, {}, {})[0], 415)
        self.assertFalse(self.args.exists())

    def test_runs_dc_commands_during_workload(self):
        # Report an empty directory to preparation. Keep 'tpcc run' alive until the stop request.
        self.cli.write_text(
            '#!/bin/sh\n'
            'for arg do\n'
            '  case "$arg" in\n'
            '    ls) echo "[]"; exit 0 ;;\n'
            '    run) exec sleep 60 ;;\n'
            '  esac\n'
            'done\n'
            'printf "%s\\n" "$@" > "$0.args"\n'
        )
        self.start_server()
        headers = {"Content-Type": "application/json"}
        self.assertEqual(self.request("POST", "/api/workload/stop", {}, headers)[0], 409)
        status, started = self.request("POST", "/api/workload/start", {}, headers)
        self.assertEqual(status, 202, started)
        self.assertEqual(started["phase"], "preparing")

        def wait_for_phase(expected):
            deadline = time.monotonic() + 5
            while True:
                code, workload = self.request("GET", "/api/workload")
                self.assertEqual(code, 200)
                if workload["phase"] == expected:
                    return
                if workload["phase"] == "failed" or time.monotonic() >= deadline:
                    self.fail(f"expected {expected}, received {workload}")
                time.sleep(0.01)

        wait_for_phase("running")
        self.assertEqual(self.request("POST", "/api/workload/start", {}, headers)[0], 409)
        for action in ("takedown", "rejoin"):
            status, result = self.action(action)
            self.assertEqual(status, 200, result)
            self.assertEqual(self.args.read_text().splitlines()[-2:], [action, "--datacenter-name=zone-a"])
        self.assertEqual(self.request("GET", "/api/workload")[1]["phase"], "running")
        status, stopping = self.request("POST", "/api/workload/stop", {}, headers)
        self.assertEqual(status, 202, stopping)
        self.assertEqual(stopping["phase"], "stopping")
        wait_for_phase("stopped")

    def test_storage_without_monitoring_url(self):
        self.start_server()
        status, result = self.request("GET", "/api/storage")
        self.assertEqual(status, 502)
        self.assertIn("--monitoring-url", result["error"])

    def test_storage_combines_viewer_disks_with_current_distconf(self):
        monitoring_url = "http://viewer.example.test:8765/"
        self.start_server(monitoring_url=monitoring_url)
        group = {
            "GroupID": 0,
            "GroupGeneration": 2,
            "Overall": "Yellow",
            "ErasureSpecies": "mirror-3-dc",
            "VDiskNodeIds": [1, 4, 7],
            "VDisks": [
                {
                    "VDiskId": {"GroupID": 0, "GroupGeneration": 2, "Ring": 0, "Domain": 0, "VDisk": 0},
                    "NodeId": 1,
                    "VDiskState": "OK",
                    "Replicated": True,
                },
                {"VDiskId": {"GroupID": 0, "GroupGeneration": 2, "Ring": 1, "Domain": 0, "VDisk": 0}},
            ],
        }
        viewer = {"StoragePools": [{"Name": "static", "Groups": [group]}]}
        static = {"GroupID": 0, "GroupGeneration": 2, "Mirror3dcState": "DC_FAILURE"}
        config = {
            "AllNodes": [
                {"NodeId": node, "Location": {"DataCenter": dc, "Rack": f"rack-{node}"}}
                for node, dc in ((1, "dc1"), (4, "dc2"), (7, "dc3"))
            ],
            "DataCenterStates": {"DataCenter": [{"Name": "dc2", "State": "EXCLUDED"}]},
            "BlobStorageConfig": {"ServiceSet": {"Groups": [static]}},
        }
        with patch.object(storage, "_read_json", side_effect=[viewer, {"QueryConfig": {"Config": config}}]) as read:
            status, snapshot = self.request("GET", "/api/storage")
            self.assertEqual(status, 200, snapshot)
            self.assertEqual(
                [call.args for call in read.call_args_list],
                [
                    (monitoring_url + "viewer/json/storage?groups=true",),
                    (monitoring_url + "actors/nodewarden?page=distconf", {"QueryConfig": {}}),
                ],
            )
        self.assertEqual(snapshot["monitoringUrl"], monitoring_url)
        (actual,) = snapshot["groups"]
        self.assertEqual((actual["id"], actual["generation"], actual["state"]), (0, 2, "DC_FAILURE"))
        realms = {realm["name"]: realm for realm in actual["realms"]}
        self.assertEqual(
            {dc: realm["policy"] for dc, realm in realms.items()},
            {
                "dc1": "INCLUDED",
                "dc2": "EXCLUDED",
                "dc3": "INCLUDED",
            },
        )
        for dc, node, state, replicated in (("dc1", 1, "OK", True), ("dc2", 4, None, None), ("dc3", 7, None, None)):
            with self.subTest(dc=dc):
                (disk,) = realms[dc]["disks"]
                self.assertEqual((disk["node"], disk["state"], disk["replicated"]), (node, state, replicated))
        static["GroupGeneration"] = 1
        self.assertIsNone(storage._groups(viewer, config)[0]["state"])

    def test_storage_uses_current_acknowledged_bsc_group_state(self):
        monitoring_url = "http://viewer.example.test:8765/"
        dynamic_id = 2181038080
        static = {"GroupID": 0, "GroupGeneration": 2, "ErasureSpecies": "mirror-3-dc"}
        dynamic = {
            "GroupID": dynamic_id,
            "GroupGeneration": 3,
            "ErasureSpecies": "mirror-3-dc",
            "VDiskNodeIds": [1],
            "VDisks": [{"NodeId": 1, "VDiskState": "OK", "Replicated": True}],
        }
        viewer = {"StoragePools": [{"Name": "static", "Groups": [static]}, {"Name": "dc-admin", "Groups": [dynamic]}]}
        distconf = {
            "QueryConfig": {
                "Config": {
                    "BlobStorageConfig": {"ServiceSet": {"Groups": [dict(static, Mirror3dcState="DC_RECOVERY_INV1")]}},
                    "AllNodes": [{"NodeId": 1, "Location": {"DataCenter": "dc1"}}],
                }
            }
        }
        bsc_group = {"GroupId": dynamic_id, "GroupGeneration": 3, "Mirror3dcState": "DC_FAILURE_INV1"}
        base_config = {"BaseConfig": {"Group": [bsc_group]}}
        for scenario, response, expected in (
            ("current", {"Success": True, "Status": [dict(base_config, Success=True)]}, "DC_FAILURE_INV1"),
            (
                "stale",
                {
                    "Success": True,
                    "Status": [
                        {"Success": True, "BaseConfig": {"Group": [dict(bsc_group, GroupGeneration=2)]}},
                    ],
                },
                None,
            ),
            ("request rejected", {"Success": False, "Status": [dict(base_config, Success=True)]}, None),
            ("command rejected", {"Success": True, "Status": [dict(base_config, Success=False)]}, None),
            ("unavailable", OSError("BSC unavailable"), None),
        ):
            with self.subTest(scenario=scenario):
                with patch.object(storage, "_read_json", side_effect=[viewer, distconf, response]) as read:
                    groups = storage.StorageViewer(monitoring_url).read()["groups"]
                self.assertEqual(
                    read.call_args.args,
                    (
                        monitoring_url + "tablets/app?TabletID=72057594037932033&exec=1",
                        {
                            "Command": [
                                {
                                    "QueryBaseConfig": {
                                        "SuppressPDisks": True,
                                        "SuppressVSlots": True,
                                        "SuppressNodes": True,
                                    }
                                }
                            ]
                        },
                    ),
                )
                self.assertEqual(
                    [(g["id"], g["state"]) for g in groups], [(0, "DC_RECOVERY_INV1"), (dynamic_id, expected)]
                )
                (realm,) = groups[1]["realms"]
                self.assertEqual(realm["name"], "dc1")
                self.assertEqual(realm["disks"], [{"node": 1, "domain": None, "state": "OK", "replicated": True}])
