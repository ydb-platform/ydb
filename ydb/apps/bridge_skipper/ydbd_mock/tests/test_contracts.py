"""Direct public-wire contracts migrated from acceptance/lib/contracts.py.

These tests depend only on the mock testkit and generated public protobufs.
"""
import asyncio
import unittest

import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings, wait_until
from ydb.public.api.protos.draft import ydb_bridge_pb2 as B


class WireContractsTest(unittest.IsolatedAsyncioTestCase):
    async def make(self, initial=None):
        c = LiveCluster(pile_names=["a", "b", ""], initial=initial,
                        timings=tiny_timings(lifecycle_delay=0.05))
        self.addAsyncCleanup(c.stop)
        await c.start()
        return c

    async def call(self, c, method, request, source="a", status=model.SUCCESS):
        response = await getattr(c.bridge(c.label(source)), method)(request, timeout=4)
        self.assertEqual(response.operation.status, status, operations.issue_texts(response.operation))
        return response.operation

    async def state(self, c, source="a"):
        return operations.unpack(await self.call(c, "GetClusterState", B.GetClusterStateRequest(), source),
                                 B.GetClusterStateResult)

    async def states(self, c, source="a"):
        return {p.pile_name: p.state for p in (await self.state(c, source)).pile_states}

    async def update(self, c, updates, source="a", status=model.SUCCESS, expected=None):
        request = B.UpdateClusterStateRequest()
        for name, state in updates.items():
            request.updates.add(pile_name=name, state=state)
        if expected is not None:
            request.expected_generation = expected
        return await self.call(c, "UpdateClusterState", request, source, status)

    async def gates(self, c, **body):
        for name in c.topo.pile_names:
            status, result = await c.http(name, "POST", "/followups", body)
            self.assertEqual(status, 200, result)

    async def test_exact_enum_error(self):
        c = await self.make()
        before = await self.state(c)
        op = await self.update(c, {"b": 999}, status=model.INTERNAL_ERROR)
        self.assertEqual(operations.issue_texts(op), ["Unsupported pile state"])
        self.assertEqual(await self.state(c), before)

    async def test_batch_disconnect_is_one_generation(self):
        c = await self.make()
        before = await self.state(c)
        await self.update(c, {"b": model.PUB_DISCONNECTED, "": model.PUB_DISCONNECTED})
        self.assertEqual(await self.states(c),
                         {"a": model.PUB_PRIMARY, "b": model.PUB_DISCONNECTED, "": model.PUB_DISCONNECTED})
        self.assertEqual((await self.state(c)).generation, before.generation + 1)

    async def test_omitted_internal_stage2_is_preserved(self):
        c = await self.make()
        await self.gates(c, hold=["sync_step"])
        await self.update(c, {"": model.PUB_DISCONNECTED})
        await self.update(c, {"": model.PUB_NOT_SYNCHRONIZED})
        await wait_until(lambda: c.inst("a").state.stored(2) == model.Stored.NOT_SYNCHRONIZED_2,
                         message="internal stage2 fixture evidence")
        self.assertEqual((await self.states(c))[""], model.PUB_NOT_SYNCHRONIZED)
        await self.update(c, {"b": model.PUB_DISCONNECTED})
        self.assertEqual(c.inst("a").state.stored(2), model.Stored.NOT_SYNCHRONIZED_2)
        self.assertEqual((await self.states(c))[""], model.PUB_NOT_SYNCHRONIZED)

    async def test_implicit_generation_rejection_from_a_stale_receiver(self):
        c = await self.make()
        # A disconnected pile cannot pull the newer history back in. Restore its
        # complete older snapshot, including the storage-config revision.
        stale = c.inst("b").state.copy()
        await self.update(c, {"b": model.PUB_DISCONNECTED})
        authority = await self.state(c)
        c.inst("b").replica.force(stale, detached=False)
        c.inst("b").tracker.reset_from(stale)
        await c.procs["b"].reconcile_listeners(grace=0)
        self.assertEqual(c.inst("b").state.version, stale.version)
        op = await self.update(c, {"": model.PUB_DISCONNECTED}, source="b", status=model.INTERNAL_ERROR)
        self.assertTrue(any("New cluster state generation#" in issue for issue in operations.issue_texts(op)))
        self.assertEqual(await self.state(c), authority)

    async def test_abandoned_promoted_is_explicitly_demoted(self):
        c = await self.make()
        await self.gates(c, hold=["promote"])
        await self.update(c, {"": model.PUB_PROMOTED})
        await self.update(c, {"a": model.PUB_DISCONNECTED, "b": model.PUB_PRIMARY,
                              "": model.PUB_SYNCHRONIZED})
        self.assertEqual(await self.states(c, "b"),
                         {"a": model.PUB_DISCONNECTED, "b": model.PUB_PRIMARY, "": model.PUB_SYNCHRONIZED})

    async def test_add_remove_completion_generation_and_errors(self):
        c = await self.make(initial={"configured_piles": ["a"]})
        status, result = await c.http("a", "POST", "/topology", {"configured_piles": ["a", "b", ""]})
        self.assertEqual(status, 200, result)
        self.assertEqual((await self.states(c))[""], model.PUB_INACTIVE)
        await self.gates(c, error_reason="conformance allocation error")
        await self.call(c, "AddPile", B.AddPileRequest(pile_name=""), status=model.GENERIC_ERROR)
        pending = await self.state(c)
        self.assertEqual((pending.pending_pile_operation.pile_name, pending.pending_pile_operation.kind,
                          pending.pending_pile_operation.error_reason),
                         ("", B.PendingPileOperation.ADD, "conformance allocation error"))
        generation = pending.generation
        await self.gates(c, error_reason=None)
        await wait_until(lambda: c.inst("a").state.pending_pile_operation is None, message="Add completion")
        self.assertEqual((await self.state(c)).generation, generation)
        self.assertEqual((await self.states(c))[""], model.PUB_DISCONNECTED)
        await self.gates(c, hold=["remove"])
        task = asyncio.create_task(self.call(c, "RemovePile", B.RemovePileRequest(pile_name="")))
        try:
            await wait_until(lambda: c.inst("a").state.pending_pile_operation is not None, message="Remove pending")
            self.assertEqual((await self.state(c)).generation, generation)
            await self.gates(c, release=["remove"])
            await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual((await self.states(c))[""], model.PUB_INACTIVE)
        self.assertEqual((await self.state(c)).generation, generation + 1)

    async def test_rejoin_exact_empty_name(self):
        c = await self.make()
        await self.update(c, {"": model.PUB_DISCONNECTED})
        await self.update(c, {"": model.PUB_NOT_SYNCHRONIZED})

        async def synchronized():
            return (await self.states(c))[""] == model.PUB_SYNCHRONIZED

        await wait_until(synchronized, message="empty-name rejoin")

    async def test_disconnected_listener_closes(self):
        c = await self.make()
        await self.update(c, {"": model.PUB_DISCONNECTED})

        async def refused():
            try:
                await c.bridge(c.label("")).GetClusterState(B.GetClusterStateRequest(), timeout=0.3)
            except grpc.aio.AioRpcError as error:
                return error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
            return False

        await wait_until(refused, message="disconnected listener closure")
        self.assertTrue((await self.state(c)).pile_states)

    async def test_guard_aborts_when_current_is_newer_without_applying(self):
        c = await self.make()
        await self.update(c, {"b": model.PUB_DISCONNECTED}, expected=1)
        before = await self.state(c)
        await self.update(c, {"b": model.PUB_NOT_SYNCHRONIZED}, expected=1, status=model.ABORTED)
        self.assertEqual(await self.state(c), before)

    async def test_guard_on_lagging_receiver_is_unavailable_without_applying(self):
        c = await self.make()
        await self.update(c, {"b": model.PUB_DISCONNECTED}, expected=1)
        authority = await self.state(c)
        c.inst("b").override_replica({"b": model.PUB_SYNCHRONIZED}, generation=1)
        await c.procs["b"].reconcile_listeners(grace=0)
        await self.update(c, {"": model.PUB_DISCONNECTED}, source="b", expected=authority.generation,
                          status=model.UNAVAILABLE)
        self.assertEqual((await self.state(c, "b")).generation, 1)
        self.assertEqual(await self.state(c), authority)

    async def test_guard_commit_generation_is_expected_plus_one(self):
        c = await self.make()
        generation = (await self.state(c)).generation
        await self.update(c, {"b": model.PUB_DISCONNECTED}, expected=generation)
        self.assertEqual((await self.state(c)).generation, generation + 1)
