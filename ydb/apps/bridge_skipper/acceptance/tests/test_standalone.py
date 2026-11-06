"""Standalone mock processes and the C++ controller launcher."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

import yatest.common

from library.python.port_manager import PortManager
from ydb.apps.bridge_skipper.acceptance.lib.harness import http_request
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology


class StandaloneTest(unittest.TestCase):
    def test_mock_cluster_and_cpp_skipper_launch(self):
        with tempfile.TemporaryDirectory() as run, PortManager() as ports:
            self._launch(run, ports)

    def _launch(self, run, ports):
        topology = Topology(["a", "b"], 2, control_ports=[ports.get_port(), ports.get_port()],
                            grpc_ports=[[ports.get_port(), ports.get_port()], [ports.get_port(), ports.get_port()]])
        topology_path = str(Path(run) / "input.yaml")
        topology.save(topology_path)
        mock_binary = yatest.common.binary_path("ydb/apps/bridge_skipper/ydbd_mock/ydbd_mock")
        skipper_binary = yatest.common.binary_path("ydb/apps/bridge_skipper/bridge_skipper")
        env = dict(os.environ)
        env.pop("Y_PYTHON_ENTRY_POINT", None)
        command = [mock_binary, "cluster", "--run-dir", run]
        controller = None
        try:
            subprocess.run(command + ["up", "--topology", topology_path, "--fresh"],
                           env=env, check=True, capture_output=True, timeout=30)
            port = ports.get_port()
            with open(Path(run) / "controller.log", "w+") as log:
                controller = subprocess.Popen(command + ["skipper", "--binary", skipper_binary,
                                              "--listen", "127.0.0.1:%d" % port],
                                              env=env, stdout=log, stderr=log)
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if controller.poll() is not None:
                        log.seek(0)
                        self.fail("controller exited: " + log.read())
                    try:
                        status, state, _ = http_request("http://127.0.0.1:%d/api/state" % port)
                        if status == 200 and state["view"] is not None:
                            self.assertEqual(state["mode"], "manual")
                            config = json.loads(next(Path(run).glob("bridge_skipper/*/config.json")).read_text())
                            self.assertEqual(state["cluster_id"], config["cluster_id"])
                            self.assertEqual({endpoint["uri"] for endpoint in state["endpoints"]},
                                             {"grpc://" + node.endpoint for node in topology.all_nodes()})
                            return
                    except OSError:
                        pass
                    time.sleep(0.1)
                self.fail("controller did not observe the standalone mock cluster")
        finally:
            if controller is not None and controller.poll() is None:
                controller.terminate()
                try:
                    controller.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    controller.kill()
                    controller.wait(timeout=5)
            subprocess.run(command + ["down"], env=env, check=True, capture_output=True, timeout=30)
