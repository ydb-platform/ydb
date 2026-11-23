"""The browser lab controls real node listeners and owns only its own processes."""
import asyncio
from contextlib import ExitStack
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest import mock

import grpc
from aiohttp import ClientSession, web

from ydb.apps.bridge_skipper.ydbd_mock.lib import lab, model, supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, wait_until
from ydb.public.api.protos.draft import ydb_bridge_pb2


class ControlUiTest(unittest.IsolatedAsyncioTestCase):
    async def test_page_and_host_crash_start_use_the_existing_mock_controls(self):
        cluster = LiveCluster()
        await cluster.start()
        self.addAsyncCleanup(cluster.stop)
        managed = mock.Mock()
        managed.manifest.return_value = {"cluster_id": "test"}
        managed.pile.side_effect = lambda name: name
        managed.topology = cluster.topo
        # The supervisor's synchronous client runs in a worker thread while the mocks run here.
        managed.request.side_effect = lambda pile, method, path, body: supervisor.Cluster.request(
            managed, pile, method, path, body)
        runner = web.AppRunner(supervisor.build_http_app(managed))
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        async with ClientSession() as client:
            base = "http://127.0.0.1:%d" % port
            async with client.get(base + "/") as response:
                self.assertEqual(response.status, 200)
                page = await response.text()
                self.assertIn("ydbd_mocks_control", page)
                self.assertIn("Crash", page)
                self.assertIn("Start", page)
            node = cluster.topo.all_nodes()[0]

            async def reachable():
                try:
                    response = await cluster.bridge(node.label).GetClusterState(
                        ydb_bridge_pb2.GetClusterStateRequest(), timeout=1)
                    return response.operation.status == model.SUCCESS
                except grpc.aio.AioRpcError:
                    return False

            for down in (True, False):
                async with client.post(base + "/api/v1/fault", json={
                        "target": node.label, "op": "down", "on": down}) as response:
                    self.assertEqual(response.status, 200, await response.text())
                state = await cluster.state(node.pile_name)
                status = next(item for item in state["nodes"] if item["label"] == node.label)
                self.assertEqual(bool(status["faults"].get("down")), down)
                if down:
                    with self.assertRaises(grpc.aio.AioRpcError) as error:
                        await cluster.bridge(node.label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=1)
                    self.assertEqual(error.exception.code(), grpc.StatusCode.UNAVAILABLE)
                else:
                    await wait_until(reachable, timeout=8, message="host listener restored")


class LabLauncherTest(unittest.TestCase):
    def args(self, run):
        return supervisor.build_parser().parse_args([
            "--run-dir", run, "lab", "--binary", "/fake/skipper"])

    def test_existing_cluster_is_preserved_when_up_refuses(self):
        with tempfile.TemporaryDirectory() as run:
            cluster = supervisor.Cluster(run)
            with mock.patch.object(lab, "check_binary"), mock.patch.object(
                    supervisor, "cmd_up", side_effect=supervisor.SupervisorError("already running")), mock.patch.object(
                    supervisor, "cmd_down") as down:
                with self.assertRaisesRegex(supervisor.SupervisorError, "already running"):
                    asyncio.run(lab.run(cluster, self.args(run)))
                down.assert_not_called()

    def test_skipper_startup_failure_cleans_up_started_mocks(self):
        with tempfile.TemporaryDirectory() as run:
            cluster = supervisor.Cluster(run)
            with mock.patch.object(lab, "check_binary"), mock.patch.object(
                    supervisor, "cmd_up"), mock.patch.object(supervisor, "cmd_down") as down, mock.patch.object(
                    lab, "start_services", side_effect=supervisor.SupervisorError("skipper exited")):
                with self.assertRaisesRegex(supervisor.SupervisorError, "skipper exited"):
                    asyncio.run(lab.run(cluster, self.args(run)))
                down.assert_called_once()

    def test_missing_binary_fails_before_starting_mocks(self):
        with self.assertRaisesRegex(supervisor.SupervisorError, "executable"):
            lab.check_binary("/missing/bridge_skipper")


class LabStartupOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.context = ExitStack()
        self.addCleanup(self.context.close)
        self.cluster = supervisor.Cluster(self.context.enter_context(tempfile.TemporaryDirectory()))
        self.children = []
        self.addCleanup(self.stop_children)
        popen = supervisor.subprocess.Popen

        def spawn(*args, **kwargs):
            child = popen(*args, **kwargs)
            self.children.append(child)
            return child

        # Real child processes exercise ownership without binding any mock ports.
        self.context.enter_context(mock.patch.dict(os.environ, Y_PYTHON_ENTRY_POINT=":main"))
        self.context.enter_context(mock.patch.object(supervisor, "argv_prefix", return_value=[
            sys.executable, "-c", "import time; time.sleep(60)"]))
        self.context.enter_context(mock.patch.object(supervisor.subprocess, "Popen", side_effect=spawn))
        self.context.enter_context(mock.patch.object(self.cluster, "wait_responsive"))
        self.context.enter_context(mock.patch.object(lab, "check_binary"))

    def stop_children(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.wait()

    def assert_startup_rollback(self, child_count):
        args = supervisor.build_parser().parse_args(["lab", "--binary", "/fake/skipper"])
        with self.assertRaisesRegex(supervisor.SupervisorError, "injected startup failure"):
            asyncio.run(lab.run(self.cluster, args))
        self.assertEqual(len(self.children), child_count)
        self.assertTrue(all(child.poll() is not None for child in self.children),
                        "failed startup left an owned child running")
        self.assertEqual(list(Path(self.cluster.path("pids")).glob("*.pid")), [])

    def fail_open(self, path):
        original = open

        def failing_open(file, *args, **kwargs):
            if os.fspath(file) == path:
                raise PermissionError("injected startup failure")
            return original(file, *args, **kwargs)

        return mock.patch("builtins.open", side_effect=failing_open)

    def test_later_log_open_error_rolls_back_earlier_children(self):
        with self.fail_open(self.cluster.path("logs", "pile2.log")):
            self.assert_startup_rollback(1)

    def test_pid_write_error_rolls_back_the_just_spawned_child(self):
        with self.fail_open(self.cluster.path("pids", "pile1.pid")):
            self.assert_startup_rollback(1)

    def test_ready_output_error_rolls_back_all_started_children(self):
        with mock.patch.object(supervisor, "_print", side_effect=BrokenPipeError("injected startup failure")):
            self.assert_startup_rollback(2)


class LabReadinessTest(unittest.IsolatedAsyncioTestCase):
    async def test_restart_after_an_open_browser_connection(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            port = listener.getsockname()[1]
            with socket.create_connection(("127.0.0.1", port)) as client:
                connection, _ = listener.accept()
                connection.shutdown(socket.SHUT_WR)
                self.assertEqual(client.recv(1), b"")
                connection.close()
        with tempfile.TemporaryDirectory() as run:
            cluster = supervisor.Cluster(run)
            Path(cluster.path("logs")).mkdir()
            args = self.args(port)
            stopped = asyncio.Event()
            stopped.set()
            with mock.patch.object(supervisor, "build_http_app", return_value=web.Application()), mock.patch.object(
                    lab.subprocess, "Popen") as spawn, mock.patch.object(
                    lab, "_wait_skipper", new=mock.AsyncMock(return_value=False)):
                await lab.start_services(cluster, args, stopped)
                spawn.assert_called_once()

    def args(self, port):
        return supervisor.build_parser().parse_args([
            "lab", "--binary", "/fake/skipper", "--control-port", "0", "--skipper-port", str(port)])

    async def test_busy_skipper_port_does_not_start_a_child(self):
        with socket.socket() as listener, tempfile.TemporaryDirectory() as run:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            with mock.patch.object(supervisor, "build_http_app", return_value=web.Application()), mock.patch.object(
                    lab.subprocess, "Popen") as spawn:
                with self.assertRaisesRegex(supervisor.SupervisorError, "port"):
                    await lab.start_services(supervisor.Cluster(run), self.args(listener.getsockname()[1]),
                                             asyncio.Event())
                spawn.assert_not_called()

    async def test_stop_before_ready_does_not_report_a_child_crash(self):
        stopped = asyncio.Event()
        stopped.set()
        proc = mock.Mock(returncode=0)
        proc.poll.return_value = 0
        self.assertFalse(await lab._wait_skipper(proc, "http://127.0.0.1:1", stopped))

    async def test_stop_interrupts_the_readiness_wait(self):
        stopped = asyncio.Event()
        proc = mock.Mock()
        proc.poll.return_value = None
        task = asyncio.create_task(lab._wait_skipper(proc, "http://127.0.0.1:1", stopped, timeout=5))
        await asyncio.sleep(0.01)
        stopped.set()
        self.assertFalse(await asyncio.wait_for(task, timeout=1))
