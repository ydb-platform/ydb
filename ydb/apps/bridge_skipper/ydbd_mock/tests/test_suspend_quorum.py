"""A suspended pile cannot finish takedown after losing its static quorum."""
import asyncio
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.followups import Due, SUSPEND, build_commit
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings, wait_until
from ydb.public.api.protos.draft import ydb_bridge_pb2 as B


class SuspendQuorumTest(unittest.IsolatedAsyncioTestCase):
    async def test_suspension_stays_pending_until_the_suspended_pile_recovers(self):
        c = LiveCluster(timings=tiny_timings(suspend_delay=0.1))
        self.addAsyncCleanup(c.stop)
        await c.start()
        for name in c.topo.pile_names:
            c.inst(name).control_followups({"hold": ["suspend"]})
        request = B.UpdateClusterStateRequest(expected_generation=1)
        request.updates.add(pile_name="pile2", state=model.PUB_SUSPENDED)
        response = await c.bridge(c.label("pile1")).UpdateClusterState(request, timeout=2)
        self.assertEqual(response.operation.status, model.SUCCESS)
        await wait_until(
            lambda: all(c.inst(p).state.stored(1) == model.Stored.SUSPENDED
                        for p in c.topo.pile_names), message="suspended state delivered")
        for i in (0, 1):
            await c.procs["pile2"].set_node_down(i, True)
        await c.inst("pile1").heartbeat_once()
        for name in c.topo.pile_names:
            c.inst(name).control_followups({"release": ["suspend"]})
        await asyncio.sleep(0.35)
        for name in c.topo.pile_names:
            self.assertEqual(c.inst(name).state.stored(1), model.Stored.SUSPENDED)
            self.assertEqual(c.inst(name).state.generation, 2)
        request.expected_generation = 2
        request.updates[0].state = model.PUB_DISCONNECTED
        response = await c.bridge(c.label("pile1")).UpdateClusterState(request, timeout=2)
        self.assertEqual(response.operation.status, model.BAD_REQUEST,
                         "the public API cannot recover a failed SUSPENDED pile")
        await c.procs["pile2"].set_node_down(0, False)
        await wait_until(lambda: c.inst("pile1").state.stored(1) == model.Stored.DISCONNECTED,
                         message="suspension finishes after node quorum returns")

    async def test_suspend_admission_requires_a_live_prepare_despite_cached_quorum(self):
        c = LiveCluster(timings=tiny_timings(suspend_delay=60, heartbeat_interval=60))
        self.addAsyncCleanup(c.stop)
        await c.start()
        for name in c.topo.pile_names:
            c.inst(name).control_followups({"hold": ["suspend"]})
        request = B.UpdateClusterStateRequest(expected_generation=1)
        request.updates.add(pile_name="pile2", state=model.PUB_SUSPENDED)
        response = await c.bridge(c.label("pile1")).UpdateClusterState(request, timeout=2)
        self.assertEqual(response.operation.status, model.SUCCESS)
        source = c.inst("pile1")
        await source.heartbeat_once()
        self.assertTrue(source.has_static_quorum(1))
        new = build_commit(source.state, Due(SUSPEND, 1))
        # Lose live node quorum without giving the source another heartbeat sample.
        for f in c.inst("pile2").node_faults[:2]:
            f.down = True
        self.assertTrue(source.has_static_quorum(1), "the regression requires a stale positive sample")
        outcome = await source._two_phase(new, SUSPEND)
        self.assertFalse(outcome.committed)
        self.assertEqual(outcome.refusals[1]["reason"], "not_ready")
        self.assertEqual(source.state.stored(1), model.Stored.SUSPENDED)
        self.assertEqual(source.state.generation, 2)

    async def test_suspend_can_finish_after_its_live_prepare_was_admitted(self):
        c = LiveCluster(timings=tiny_timings(suspend_delay=60, heartbeat_interval=60))
        self.addAsyncCleanup(c.stop)
        await c.start()
        for name in c.topo.pile_names:
            c.inst(name).control_followups({"hold": ["suspend"]})
        request = B.UpdateClusterStateRequest(expected_generation=1)
        request.updates.add(pile_name="pile2", state=model.PUB_SUSPENDED)
        response = await c.bridge(c.label("pile1")).UpdateClusterState(request, timeout=2)
        self.assertEqual(response.operation.status, model.SUCCESS)
        source = c.inst("pile1")
        original_call = source.transport.call

        async def lose_quorum_after_prepare(src, dst, op, payload, timeout):
            reply = await original_call(src, dst, op, payload, timeout)
            if dst == 1 and op == "prepare" and reply.get("ok"):
                for f in c.inst("pile2").node_faults[:2]:
                    f.down = True
            return reply

        source.transport.call = lose_quorum_after_prepare
        outcome = await source._two_phase(build_commit(source.state, Due(SUSPEND, 1)), SUSPEND)
        self.assertTrue(outcome.committed)
        self.assertFalse(c.inst("pile2").ready())
        self.assertEqual(source.state.stored(1), model.Stored.DISCONNECTED)
