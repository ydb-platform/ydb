"""Standalone mock processes and the C++ controller launcher."""
import json
import os
import signal
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

    def test_lab_hangup_stops_its_detached_skipper_and_mock_children(self):
        with tempfile.TemporaryDirectory() as run, PortManager() as ports:
            topology = Topology(["a", "b"], 1, control_ports=[ports.get_port(), ports.get_port()],
                                grpc_ports=[[ports.get_port()], [ports.get_port()]])
            topology_path = str(Path(run) / "input.yaml")
            topology.save(topology_path)
            mock_binary = yatest.common.binary_path("ydb/apps/bridge_skipper/ydbd_mock/ydbd_mock")
            skipper_binary = yatest.common.binary_path("ydb/apps/bridge_skipper/bridge_skipper")
            env = dict(os.environ)
            env.pop("Y_PYTHON_ENTRY_POINT", None)
            env["YDBD_MOCK_EXE"] = mock_binary
            command = [mock_binary, "cluster", "--run-dir", run]
            owner = None
            log_path = Path(run) / "lab.log"
            try:
                with log_path.open("w") as log:
                    owner = subprocess.Popen(command + [
                        "lab", "--binary", skipper_binary, "--topology", topology_path, "--fresh",
                        "--skipper-port", str(ports.get_port()), "--control-port", str(ports.get_port())],
                        env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                    deadline = time.monotonic() + 30
                    while "Ctrl-C stops this lab." not in log_path.read_text():
                        self.assertIsNone(owner.poll(), log_path.read_text())
                        self.assertLess(time.monotonic(), deadline, "lab never became ready: " + log_path.read_text())
                        time.sleep(0.05)
                    children = self._running_lab_children(run)
                    self.assertEqual(len(children), 3, "two mocks and the exec'd skipper must be running")
                    self.assertEqual(len(list((Path(run) / "pids").glob("*.pid"))), 2)
                    owner.send_signal(signal.SIGHUP)
                    code = owner.wait(timeout=30)
                    remaining = self._running_lab_children(run)
                    self.assertEqual((code, remaining, list((Path(run) / "pids").glob("*.pid"))), (0, set(), []),
                                     "hangup must reap owned children and remove pid files; log:\n" + log_path.read_text())
            finally:
                # RED also leaves detached children: identify only this unique run,
                # and terminate them before releasing ports or the run directory.
                try:
                    if owner is not None and owner.poll() is None:
                        owner.terminate()
                        try:
                            owner.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            owner.kill()
                            owner.wait(timeout=5)
                finally:
                    try:
                        subprocess.run(command + ["down"], env=env, check=True, capture_output=True, timeout=30)
                    finally:
                        deadline = time.monotonic() + 5
                        children = self._running_lab_children(run)
                        while children:
                            for pid in children:
                                try:
                                    os.kill(pid, signal.SIGKILL)
                                except ProcessLookupError:
                                    pass
                            self.assertLess(time.monotonic(), deadline, "test cleanup left a live owned child")
                            time.sleep(0.05)
                            children = self._running_lab_children(run)

    def _running_lab_children(self, run):
        # The public launchers include the unique run in every child's arguments.
        # Match those arguments rather than trusting a stale pid file or a port.
        markers = ("--topology %s " % (Path(run) / "topology.yaml"),
                   "--config %s/" % (Path(run) / "bridge_skipper"))
        result = subprocess.run(["ps", "-ww", "-axo", "pid=,stat=,command="],
                                check=True, capture_output=True, text=True, timeout=5)
        children = set()
        for row in result.stdout.splitlines():
            fields = row.strip().split(None, 2)
            if len(fields) != 3:
                continue
            pid, status, command = fields
            if not status.startswith("Z") and any(marker in command + " " for marker in markers):
                children.add(int(pid))
        return children

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
                            self.assertEqual(state["mode"], "observe")
                            config = json.loads(next(Path(run).glob("bridge_skipper/*/config.json")).read_text())
                            self.assertNotIn("mode", config)
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
