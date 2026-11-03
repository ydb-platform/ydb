"""Node gRPC listener ownership across failed starts and cancellation."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import instance
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology, TopologyError


class ControlledGrpcServer:
    def __init__(self):
        self.start_entered = asyncio.Event()
        self.allow_start = asyncio.Event()
        self.started = asyncio.Event()
        self.start_cancelled = False
        self.stop_entered = asyncio.Event()
        self.allow_stop = asyncio.Event()
        self.stopped = asyncio.Event()
        self.stop_cancelled = False
        self.stop_calls = 0

    def add_insecure_port(self, endpoint):
        return 12345

    async def start(self):
        self.start_entered.set()
        try:
            await self.allow_start.wait()
        except asyncio.CancelledError:
            self.start_cancelled = True
            raise
        self.started.set()

    async def stop(self, grace):
        self.stop_calls += 1
        self.stop_entered.set()
        try:
            await self.allow_stop.wait()
        except asyncio.CancelledError:
            self.stop_cancelled = True
            raise
        self.stopped.set()


class NodeServerLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.topology = Topology.with_free_ports(["pile1"], 1)
        pile = self.topology.pile("pile1")
        self.process = SimpleNamespace(
            topo=self.topology,
            inst=SimpleNamespace(pile_info=pile),
            events=mock.Mock(),
            runtimes=[mock.Mock()],
        )
        self.server = ControlledGrpcServer()
        self.node = instance.NodeServer(self.process, 0)
        self.server_factory = mock.patch.object(instance.grpc.aio, "server", return_value=self.server)
        self.server_factory.start()
        self.addCleanup(self.server_factory.stop)
        self.register = mock.patch.object(instance.services, "register")
        self.register.start()
        self.addCleanup(self.register.stop)
        self.addAsyncCleanup(self._release_server)

    async def _release_server(self):
        self.server.allow_start.set()
        self.server.allow_stop.set()
        await self.node.stop()

    async def test_cancelled_start_keeps_server_owned_until_start_and_stop_finish(self):
        start_waiter = asyncio.create_task(self.node.start())
        await asyncio.wait_for(self.server.start_entered.wait(), timeout=1.0)
        start_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await start_waiter

        self.assertIs(self.node.server, self.server)
        self.assertFalse(self.server.start_cancelled)

        stop_waiter = asyncio.create_task(self.node.stop())
        await asyncio.sleep(0)
        self.assertFalse(self.server.stop_entered.is_set())

        self.server.allow_start.set()
        await asyncio.wait_for(self.server.started.wait(), timeout=1.0)
        await asyncio.wait_for(self.server.stop_entered.wait(), timeout=1.0)
        self.assertIs(self.node.server, self.server)

        self.server.allow_stop.set()
        await asyncio.wait_for(stop_waiter, timeout=1.0)
        self.assertTrue(self.server.stopped.is_set())
        self.assertIsNone(self.node.server)

    async def test_cancelled_stop_keeps_server_owned_until_shutdown_finishes(self):
        self.server.allow_start.set()
        await self.node.start()

        stop_waiter = asyncio.create_task(self.node.stop())
        await asyncio.wait_for(self.server.stop_entered.wait(), timeout=1.0)
        stop_waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await stop_waiter

        self.assertIs(self.node.server, self.server)
        self.assertFalse(self.server.stop_cancelled)

        repeated_stop = asyncio.create_task(self.node.stop())
        await asyncio.sleep(0)
        self.assertEqual(self.server.stop_calls, 1)

        self.server.allow_stop.set()
        await asyncio.wait_for(repeated_stop, timeout=1.0)
        self.assertTrue(self.server.stopped.is_set())
        self.assertIsNone(self.node.server)

    async def test_bind_failure_starts_then_stops_allocated_server(self):
        self.server.allow_start.set()
        self.server.allow_stop.set()
        with mock.patch.object(self.server, "add_insecure_port", return_value=0):
            with self.assertRaises(OSError):
                await self.node.start()

        self.assertTrue(self.server.started.is_set())
        self.assertTrue(self.server.stopped.is_set())
        self.assertIsNone(self.node.server)


class InvalidTlsStartupTest(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_tls_is_rejected_before_server_creation_and_cleans_processes(self):
        cluster = LiveCluster(tls={"cert_file": "/nonexistent/cert.pem", "key_file": "/nonexistent/key.pem"})
        self.addAsyncCleanup(cluster.stop)

        with mock.patch.object(instance.grpc.aio, "server") as server_factory:
            with self.assertRaises(TopologyError):
                await cluster.start()

        self.assertTrue(cluster.procs)
        open_runners = [name for name, process in cluster.procs.items() if process._runner is not None]
        self.assertEqual((server_factory.call_count, open_runners), (0, []))


class MockProcessShutdownTest(unittest.IsolatedAsyncioTestCase):
    async def test_stop_joins_gated_listener_start_before_closing_process_resources(self):
        topology = Topology.with_free_ports(["pile1"], 1)
        process = instance.MockProcess(topology, "pile1")
        server = ControlledGrpcServer()
        start_task = None

        async def cleanup():
            server.allow_start.set()
            server.allow_stop.set()
            if start_task is not None and not start_task.done():
                await asyncio.gather(start_task, return_exceptions=True)
            if process._runner is not None or not server.stopped.is_set():
                await process.stop(0.0)

        self.addAsyncCleanup(cleanup)
        with mock.patch.object(instance.grpc.aio, "server", return_value=server):
            with mock.patch.object(instance.services, "register"):
                start_task = asyncio.create_task(process.start())
                await asyncio.wait_for(server.start_entered.wait(), timeout=1.0)

                stop_task = asyncio.create_task(process.stop(0.0))
                await asyncio.sleep(0)
                self.assertFalse(stop_task.done())
                self.assertFalse(server.stop_entered.is_set())
                self.assertIsNotNone(process._runner)

                server.allow_start.set()
                await asyncio.wait_for(start_task, timeout=1.0)
                await asyncio.wait_for(server.stop_entered.wait(), timeout=1.0)
                self.assertIs(process.nodes[0].server, server)
                self.assertFalse(stop_task.done())

                server.allow_stop.set()
                await asyncio.wait_for(stop_task, timeout=1.0)

        self.assertTrue(server.stopped.is_set())
        self.assertIsNone(process.nodes[0].server)
        self.assertIsNone(process._runner)


if __name__ == "__main__":
    unittest.main()
