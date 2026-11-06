"""Standalone C++ skipper configuration and process launch."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology


class SkipperLauncherTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.cluster = supervisor.Cluster(temp.name)
        Topology(["a", "", "c"], 2).save(self.cluster.path("topology.yaml"))
        Path(self.cluster.path("cluster.json")).write_text(json.dumps({"cluster_id": "mock-session"}))

    def args(self, *extra):
        return supervisor.build_parser().parse_args([
            "--run-dir", self.cluster.run, "skipper", "--binary", "/mock/bridge_skipper", *extra])

    def test_launch_uses_direct_endpoints_and_cluster_specific_state(self):
        with mock.patch.object(supervisor.os, "execv") as execute:
            supervisor.cmd_skipper(self.cluster, self.args())
        binary, argv = execute.call_args.args
        self.assertEqual(binary, "/mock/bridge_skipper")
        config = json.loads(Path(argv[argv.index("--config") + 1]).read_text())
        self.assertEqual(config["cluster_id"], "mock-session")
        self.assertEqual(config["mode"], "manual")
        self.assertEqual(config["endpoints"], {
            pile.name: ["grpc://" + node.endpoint for node in pile.nodes]
            for pile in self.cluster.topology.piles})
        self.assertTrue(argv[argv.index("--state-dir") + 1].endswith("/bridge_skipper/mock-session"))

    def test_auto_and_listener_are_explicit_options(self):
        with mock.patch.object(supervisor.os, "execv") as execute:
            supervisor.cmd_skipper(self.cluster, self.args("--mode", "auto", "--listen", "127.0.0.1:18080"))
        argv = execute.call_args.args[1]
        config = json.loads(Path(argv[argv.index("--config") + 1]).read_text())
        self.assertEqual((config["mode"], config["http"]["listen"]), ("auto", "127.0.0.1:18080"))
