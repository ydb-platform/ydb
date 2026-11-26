"""Opt-in connectivity reports use node counts, hidden stages and responder links."""
import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations, services
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings
from ydb.public.api.protos.draft import ydb_bridge_pb2


class ConnectivityReportTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.c = LiveCluster(piles=3, timings=tiny_timings(connect_delay=60, suspend_delay=60))
        self.addAsyncCleanup(self.c.stop)
        await self.c.start()

    async def read(self, include=True, pile="pile1"):
        response = await self.c.bridge(self.c.label(pile)).GetClusterState(
            ydb_bridge_pb2.GetClusterStateRequest(include_pile_connectivity=include), timeout=2)
        self.assertEqual(response.operation.status, model.SUCCESS)
        return operations.unpack(response.operation, ydb_bridge_pb2.GetClusterStateResult)

    async def test_report_is_opt_in_and_counts_partial_node_loss(self):
        self.assertFalse((await self.read(False)).HasField("pile_connectivity"))
        remote = self.c.inst("pile2")
        remote.node_faults[0].down = True
        await self.c.inst("pile1").heartbeat_once()
        result = await self.read()
        report = result.pile_connectivity
        self.assertEqual(report.generation, result.generation)
        self.assertEqual(report.reporter_node_id, 1)
        self.assertTrue(report.reporter_is_root)
        self.assertFalse(report.binding_in_progress)
        piles = {p.pile_name: p for p in report.piles}
        self.assertEqual((piles["pile2"].static_nodes, piles["pile2"].connected_static_nodes), (3, 2))
        self.assertTrue(piles["pile2"].has_node_quorum)
        remote.node_faults[1].down = True
        await self.c.inst("pile1").heartbeat_once()
        part = next(p for p in (await self.read()).pile_connectivity.piles if p.pile_name == "pile2")
        self.assertEqual(part.connected_static_nodes, 1)
        self.assertFalse(part.has_node_quorum)

    async def test_unobserved_participants_mark_binding_until_sampled_or_timed_out(self):
        inst = self.c.inst("pile1")
        # Keep background heartbeats from filling the intentionally missing samples.
        inst._heartbeat_task.cancel()
        await asyncio.gather(inst._heartbeat_task, return_exceptions=True)
        inst._heartbeat_task = None
        clock = [100.0]
        inst.reach._clock = lambda: clock[0]
        inst.reach.restart()
        self.assertTrue((await self.read()).pile_connectivity.binding_in_progress)
        await inst.heartbeat_once()
        self.assertFalse((await self.read()).pile_connectivity.binding_in_progress)
        # Reset discards the previous observations, just as process startup does.
        inst.reach.restart()
        self.assertTrue((await self.read()).pile_connectivity.binding_in_progress)
        clock[0] += inst.reach.timeout + 0.01
        report = (await self.read()).pile_connectivity
        self.assertFalse(report.binding_in_progress, "never-observed lost peers must become usable evidence")
        for part in report.piles:
            if part.pile_name != "pile1":
                self.assertEqual(part.connected_static_nodes, 0)
                self.assertFalse(part.has_node_quorum)

    async def test_unobserved_excluded_or_blocked_piles_do_not_hold_binding(self):
        inst = self.c.inst("pile1")
        inst._heartbeat_task.cancel()
        await asyncio.gather(inst._heartbeat_task, return_exceptions=True)
        inst._heartbeat_task = None
        state = inst.state.copy()
        state.per_pile[1:] = [model.Stored.NOT_SYNCHRONIZED_1, model.Stored.INACTIVE]
        inst.replica.force(state, True)
        inst.reach.restart()
        self.assertFalse((await self.read()).pile_connectivity.binding_in_progress)
        state.per_pile[1:] = [model.Stored.SYNCHRONIZED, model.Stored.SYNCHRONIZED]
        inst.replica.force(state, True)
        inst.set_blocked([1, 2])
        self.assertFalse((await self.read()).pile_connectivity.binding_in_progress)

    async def test_missing_heartbeat_count_keeps_binding_and_invalidates_cached_count(self):
        inst = self.c.inst("pile1")
        inst._heartbeat_task.cancel()
        await asyncio.gather(inst._heartbeat_task, return_exceptions=True)
        inst._heartbeat_task = None
        original_call = inst.transport.call
        omit_count = [True]

        async def ping_without_count(src, dst, op, payload, timeout):
            reply = await original_call(src, dst, op, payload, timeout)
            if dst == 1 and op == "ping" and omit_count[0]:
                reply.pop("static_up", None)
            return reply

        inst.transport.call = ping_without_count
        inst.reach.restart()
        await inst.heartbeat_once()
        self.assertNotIn(1, inst.reach.last_static_up)
        self.assertTrue((await self.read()).pile_connectivity.binding_in_progress)
        omit_count[0] = False
        await inst.heartbeat_once()
        self.assertEqual(inst.reach.last_static_up[1], 3)
        self.assertFalse((await self.read()).pile_connectivity.binding_in_progress)
        omit_count[0] = True
        await inst.heartbeat_once()
        self.assertNotIn(1, inst.reach.last_static_up)
        self.assertTrue((await self.read()).pile_connectivity.binding_in_progress)

    async def test_wire_override_preserves_independent_report_generation_and_name_order(self):
        state = await self.read(False)
        names = [p.pile_name for p in state.pile_states]
        reversed_names = list(reversed(names))
        status, body = await self.c.http("pile1", "POST", "/response-override", {
            "all": True, "method": "GetClusterState", "result": {
                "generation": str(state.generation),
                "pile_states": [{"pile_name": p.pile_name, "state": model.public_name(p.state)}
                                for p in state.pile_states],
                "pile_connectivity": {
                    "generation": str(state.generation + 1), "reporter_node_id": 1,
                    "piles": [{"pile_name": name, "static_nodes": 3, "connected_static_nodes": i + 1}
                              for i, name in enumerate(reversed_names)],
                },
            },
        })
        self.assertEqual(status, 200, body)
        result = await self.read()
        self.assertEqual(result.generation, state.generation)
        self.assertEqual(result.pile_connectivity.generation, state.generation + 1)
        self.assertEqual([p.pile_name for p in result.pile_states], names)
        self.assertEqual([p.pile_name for p in result.pile_connectivity.piles], reversed_names)
        self.assertEqual({p.pile_name: p.connected_static_nodes for p in result.pile_connectivity.piles},
                         {name: i + 1 for i, name in enumerate(reversed_names)})

    async def test_hidden_participation_and_inactive_quorum(self):
        inst = self.c.inst("pile1")
        for stored, config, data in (
                (model.Stored.NOT_SYNCHRONIZED_1, False, False),
                (model.Stored.NOT_SYNCHRONIZED_2, True, True),
                (model.Stored.SUSPENDED, True, False),
                (model.Stored.DISCONNECTED, False, False),
                (model.Stored.INACTIVE, False, False)):
            with self.subTest(stored=stored):
                state = inst.state.copy()
                state.per_pile[1] = stored
                inst.replica.force(state, True)
                await inst.heartbeat_once()
                part = next(p for p in (await self.read()).pile_connectivity.piles if p.pile_name == "pile2")
                self.assertEqual((part.in_config_quorum, part.in_data_quorum), (config, data))
                if stored == model.Stored.INACTIVE:
                    self.assertFalse(part.has_node_quorum)

    async def test_binding_flap_fault_flaps_only_while_global_quorum_is_lost(self):
        inst = self.c.inst("pile1")
        await inst.heartbeat_once()
        status, body = await self.c.http("pile1", "POST", "/control", {"all": True, "op": "binding_flap", "on": True})
        self.assertEqual(status, 200, body)
        now = [1.0]
        with patch.object(services, "time", SimpleNamespace(monotonic=lambda: now[0])):
            self.assertFalse((await self.read()).pile_connectivity.binding_in_progress, "every pile has quorum")
            inst.set_blocked([1])
            for at, expected in ((1.0, True), (2.4, True), (2.6, False), (3.9, False), (4.1, True)):
                now[0] = at
                report = (await self.read()).pile_connectivity
                self.assertEqual(report.binding_in_progress, expected, "at %.1f s" % at)
            part = next(p for p in report.piles if p.pile_name == "pile2")
            self.assertEqual(part.connected_static_nodes, 0)
            self.assertFalse(part.has_node_quorum)
            self.assertTrue(part.in_config_quorum)
            status, body = await self.c.http("pile1", "POST", "/control", {"all": True, "op": "clear"})
            self.assertEqual(status, 200, body)
            now[0] = 1.0
            self.assertFalse((await self.read()).pile_connectivity.binding_in_progress, "clear stops the flapping")
