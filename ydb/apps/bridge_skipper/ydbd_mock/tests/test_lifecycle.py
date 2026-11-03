"""Current Bridge API lifecycle, distributed detail revisions and deterministic RPC faults."""
import asyncio
import tempfile
import unittest

import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib import lifecycle, model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.cluster import PileInstance
from ydb.apps.bridge_skipper.ydbd_mock.lib.peers import InMemoryNetwork
from ydb.apps.bridge_skipper.ydbd_mock.lib.replica import Replica
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings, wait_until
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Topology
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge


class RegistryTest(unittest.TestCase):
    def test_names_keep_slots_through_reorder_drop_and_reuse(self):
        names = ["a", "b", "c", "d"]
        state = lifecycle.configure(model.initial_state(4), {"configured_piles": ["a", "b", "c"]}, names, True)
        state.per_pile[1] = model.Stored.INACTIVE
        state.per_pile[2] = model.Stored.INACTIVE
        reordered = lifecycle.configure(state, {"configured_piles": ["c", "b", "a"]}, names)
        self.assertEqual(reordered.registry, ["a", "b", "c"])
        self.assertEqual(reordered.generation, state.generation)
        dropped = lifecycle.configure(reordered, {"configured_piles": ["c", "a"]}, names)
        self.assertEqual(dropped.registry, ["a", None, "c"])
        reused = lifecycle.configure(dropped, {"configured_piles": ["d", "c", "a"]}, names)
        self.assertEqual(reused.registry, ["a", "d", "c"])
        self.assertEqual(reused.generation, state.generation)
        trimmed = lifecycle.configure(reused, {"configured_piles": ["d", "a"]}, names)
        self.assertEqual(trimmed.registry, ["a", "d"])
        self.assertEqual(trimmed.generation, state.generation + 1)
        expanded = lifecycle.configure(trimmed, {"configured_piles": ["d", "a", "b"]}, names)
        self.assertEqual(expanded.registry, ["a", "d", "b"])
        self.assertEqual(expanded.generation, trimmed.generation + 1)

    def test_stale_admin_proposal_cannot_overwrite_metadata(self):
        replica = Replica(2)
        stale = model.derive(replica.state, updates={1: model.Stored.DISCONNECTED})
        detail = model.derive_metadata(replica.state)
        detail.conversion_in_progress = True
        self.assertTrue(replica.install(detail))
        self.assertEqual(replica.try_prepare("stale", stale, 0, 1), (False, "generation"))
        fresh = model.derive(replica.state, updates={1: model.Stored.DISCONNECTED})
        self.assertEqual(replica.try_prepare("fresh", fresh, 0, 1), (True, None))
        self.assertTrue(fresh.conversion_in_progress)

    def test_metadata_persists_and_older_details_are_ignored(self):
        with tempfile.TemporaryDirectory() as root:
            replica = Replica(2, root + "/replica.json")
            detail = model.derive_metadata(replica.state)
            detail.pending_pile_operation = {"kind": "REMOVE", "pile_name": "pile2", "pile_index": 1,
                                             "operation_id": 1, "error_reason": "retrying"}
            self.assertTrue(replica.install(detail))
            loaded = Replica(2, root + "/replica.json")
            self.assertTrue(loaded.load())
            self.assertEqual(loaded.state, detail)
            self.assertFalse(loaded.install(model.initial_state(2)))


class LifecycleGrpcTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clusters = []

    async def asyncTearDown(self):
        for cluster in self.clusters:
            await cluster.stop()

    async def make(self, **kwargs):
        cluster = LiveCluster(timings=tiny_timings(lifecycle_delay=0.05), **kwargs)
        self.clusters.append(cluster)
        await cluster.start()
        return cluster

    async def control(self, cluster, path, body, pile="pile1"):
        status, result = await cluster.http(pile, "POST", path, body)
        self.assertEqual(status, 200, result)
        return result

    async def gates(self, cluster, **values):
        for pile in cluster.topo.pile_names:
            await self.control(cluster, "/followups", values, pile)

    async def state(self, cluster, pile="pile1"):
        response = await cluster.bridge(cluster.label(pile)).GetClusterState(bridge.GetClusterStateRequest())
        self.assertEqual(response.operation.status, model.SUCCESS, response.operation.issues)
        return operations.unpack(response.operation, bridge.GetClusterStateResult)

    async def test_preconversion_and_same_generation_completion(self):
        cluster = await self.make(piles=3, initial={"configured_piles": ["pile1"], "bridged": False})
        stub = cluster.bridge(cluster.label("pile1"))
        before = await stub.GetClusterState(bridge.GetClusterStateRequest())
        self.assertEqual(before.operation.status, model.NOT_FOUND)
        await self.gates(cluster, hold=["convert"])
        response = await stub.ConvertToBridge(bridge.ConvertToBridgeRequest())
        self.assertEqual(response.operation.status, model.SUCCESS)
        pending = await self.state(cluster)
        self.assertTrue(pending.conversion_in_progress)
        self.assertEqual(len(pending.pile_states), 1)
        await self.gates(cluster, release=["convert"])
        await wait_until(lambda: not cluster.inst("pile1").state.conversion_in_progress)
        self.assertEqual((await self.state(cluster)).generation, pending.generation)

    async def test_refused_pull_retries_after_same_generation_registry_change(self):
        names = ["a", "b", "c", "d"]
        topo = Topology(names)
        network = InMemoryNetwork()
        source = PileInstance(topo, "a", network)
        target = PileInstance(topo, "b", network)
        network.register(0, source)
        network.register(1, target)
        current = lifecycle.configure(model.initial_state(4), {"configured_piles": ["a", "b", "c"]}, names, True)
        current.per_pile[1] = model.Stored.DISCONNECTED
        current.per_pile[2] = model.Stored.INACTIVE
        source.replica.force(current, False)
        target.replica.force(current, False)
        newer = model.derive_metadata(current)
        source.replica.force(newer, False)
        self.assertFalse(await target._pull(0, "test"))
        self.assertEqual(target._refused_pull[0], newer.version)
        configured = lifecycle.configure(newer, {"configured_piles": ["a", "b", "d"]}, names)
        self.assertEqual(configured.generation, newer.generation)
        source.replica.force(configured, False)
        target._running = True
        try:
            target._peer_versions[0] = configured.version
            target.reach.record_ok(0, True, configured.generation)
            await target._anti_entropy()
        finally:
            target._running = False
        self.assertEqual(target.state.registry, ["a", "b", "d"])
        self.assertEqual(target.state.version, configured.version)

    async def test_rpc_envelope_override_rejects_without_mutating_and_is_one_shot(self):
        cluster = await self.make()
        label = cluster.label("pile1")
        await self.control(cluster, "/rpc-override", {
            "label": label, "method": "UpdateClusterState", "once": True,
            "response": {"operation": {"ready": True, "status": "UNAVAILABLE", "issues": [{"message": "retry"}]}}})
        req = bridge.UpdateClusterStateRequest()
        req.updates.add(pile_name="pile2", state=model.PUB_DISCONNECTED)
        response = await cluster.bridge(label).UpdateClusterState(req)
        self.assertEqual(response.operation.status, model.UNAVAILABLE)
        self.assertEqual(cluster.inst("pile1").state.generation, 1)
        response = await cluster.bridge(label).UpdateClusterState(req)
        self.assertEqual(response.operation.status, model.SUCCESS)
        await self.control(cluster, "/rpc-override", {
            "label": label, "method": "GetClusterState", "once": True,
            "response": {"operation": {"ready": True, "status": "SUCCESS"}},
            "raw_result": {"type_url": "type.googleapis.com/unknown.Result", "value_base64": "AA=="}})
        response = await cluster.bridge(label).GetClusterState(bridge.GetClusterStateRequest())
        self.assertEqual(response.operation.result.type_url, "type.googleapis.com/unknown.Result")

    async def test_add_error_retry_rejoin_remove_and_registry_reorder(self):
        cluster = await self.make(piles=3, initial={"configured_piles": ["pile1"]})
        stub = cluster.bridge(cluster.label("pile1"))
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile2"]})
        self.assertEqual((await self.state(cluster)).pile_states[1].state, model.PUB_INACTIVE)
        await self.gates(cluster, error_reason="disk allocation unavailable")
        response = await stub.AddPile(bridge.AddPileRequest(pile_name="pile2"), timeout=3)
        self.assertEqual(response.operation.status, model.GENERIC_ERROR)
        pending = await self.state(cluster)
        self.assertEqual(pending.pending_pile_operation.kind, bridge.PendingPileOperation.ADD)
        self.assertEqual(pending.pending_pile_operation.error_reason, "disk allocation unavailable")
        generation = pending.generation
        duplicate = await stub.AddPile(bridge.AddPileRequest(pile_name="pile2"))
        self.assertEqual(duplicate.operation.status, model.INTERNAL_ERROR)
        await self.gates(cluster, error_reason=None)
        await wait_until(lambda: cluster.inst("pile1").state.pending_pile_operation is None)
        self.assertEqual((await self.state(cluster)).generation, generation)
        req = bridge.UpdateClusterStateRequest()
        req.updates.add(pile_name="pile2", state=model.PUB_NOT_SYNCHRONIZED)
        self.assertEqual((await stub.UpdateClusterState(req)).operation.status, model.SUCCESS)
        await wait_until(lambda: cluster.inst("pile1").state.stored(1) == model.Stored.SYNCHRONIZED)
        await self.control(cluster, "/topology", {"configured_piles": ["pile2", "pile1"]})
        self.assertEqual([p.pile_name for p in (await self.state(cluster)).pile_states], ["pile1", "pile2"])
        req.updates[0].state = model.PUB_DISCONNECTED
        self.assertEqual((await stub.UpdateClusterState(req)).operation.status, model.SUCCESS)
        response = await stub.RemovePile(bridge.RemovePileRequest(pile_name="pile2"), timeout=3)
        self.assertEqual(response.operation.status, model.SUCCESS)
        self.assertEqual((await self.state(cluster)).pile_states[1].state, model.PUB_INACTIVE)
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile3"]})
        self.assertEqual(cluster.inst("pile1").state.registry, ["pile1", "pile3"])

    async def test_add_rpc_timeout_does_not_cancel_bsc_work(self):
        cluster = await self.make(initial={"configured_piles": ["pile1"]})
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile2"]})
        await self.gates(cluster, hold=["add"])
        stub = cluster.bridge(cluster.label("pile1"))
        with self.assertRaises(grpc.aio.AioRpcError) as caught:
            await stub.AddPile(bridge.AddPileRequest(pile_name="pile2"), timeout=0.15)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertEqual((await self.state(cluster)).pending_pile_operation.kind, bridge.PendingPileOperation.ADD)
        await self.gates(cluster, release=["add"])
        await wait_until(lambda: cluster.inst("pile1").state.pending_pile_operation is None)

    async def test_same_generation_details_propagate_to_other_active_reporters(self):
        cluster = await self.make(piles=3, initial={"configured_piles": ["pile1", "pile2"]})
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile2", "pile3"]})
        await self.gates(cluster, hold=["add"])
        stub = cluster.bridge(cluster.label("pile1"))
        pending = asyncio.ensure_future(stub.AddPile(bridge.AddPileRequest(pile_name="pile3"), timeout=5))
        await wait_until(lambda: cluster.inst("pile2").state.pending_pile_operation is not None)
        generation = (await self.state(cluster, "pile2")).generation
        await self.gates(cluster, release=["add"])
        self.assertEqual((await pending).operation.status, model.SUCCESS)
        await wait_until(lambda: cluster.inst("pile2").state.pending_pile_operation is None)
        self.assertEqual((await self.state(cluster, "pile2")).generation, generation)

    async def test_operation_timeout_envelope_does_not_cancel_committed_add(self):
        cluster = await self.make(initial={"configured_piles": ["pile1"]})
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile2"]})
        await self.gates(cluster, hold=["add"])
        request = bridge.AddPileRequest(pile_name="pile2")
        request.operation_params.operation_timeout.FromMilliseconds(100)
        response = await cluster.bridge(cluster.label("pile1")).AddPile(request, timeout=3)
        self.assertEqual(response.operation.status, model.TIMEOUT)
        self.assertIsNotNone(cluster.inst("pile1").state.pending_pile_operation)
        await self.gates(cluster, release=["add"])
        await wait_until(lambda: cluster.inst("pile1").state.pending_pile_operation is None)

    async def test_pending_disappearance_without_postcondition_is_not_success(self):
        cluster = await self.make(initial={"configured_piles": ["pile1"]})
        await self.control(cluster, "/topology", {"configured_piles": ["pile1", "pile2"]})
        await self.gates(cluster, hold=["add"])
        pending = asyncio.ensure_future(cluster.inst("pile1").admin_lifecycle("add", "pile2"))
        await wait_until(lambda: cluster.inst("pile1").state.pending_pile_operation is not None)
        await self.control(cluster, "/topology", {"configured_piles": ["pile1"], "bridged": False})
        await asyncio.sleep(0)
        self.assertFalse(pending.done())
        await self.control(cluster, "/reset-prepare", {})
        await self.control(cluster, "/reset-cancel", {})
        outcome = await asyncio.wait_for(pending, 1)
        self.assertEqual(outcome.error.status, model.INTERNAL_ERROR)

    async def test_snapshot_hold_and_node_override_do_not_change_replica(self):
        cluster = await self.make()
        label = cluster.label("pile1")
        method = "/Ydb.Bridge.V1.BridgeService/GetClusterState"
        await self.control(cluster, "/failpoints", {"label": label, "method": method, "stage": "after_snapshot"})
        pending = asyncio.ensure_future(self.state(cluster))
        await wait_until(lambda: cluster.procs["pile1"].runtimes[0].failpoints_json()[0]["hits"] == 1)
        req = bridge.UpdateClusterStateRequest()
        req.updates.add(pile_name="pile2", state=model.PUB_DISCONNECTED)
        await cluster.bridge(label).UpdateClusterState(req)
        await self.control(cluster, "/failpoints", {
            "label": label, "method": method, "stage": "after_snapshot", "action": "release"})
        self.assertEqual((await pending).generation, 1)
        await self.control(cluster, "/response-override", {
            "label": label, "method": "GetClusterState", "result": {"generation": "9007199254740993"}})
        self.assertEqual((await self.state(cluster)).generation, 9007199254740993)
        self.assertEqual(cluster.inst("pile1").state.generation, 2)
        other = await cluster.bridge(cluster.label("pile1", 2)).GetClusterState(bridge.GetClusterStateRequest())
        self.assertEqual(operations.unpack(other.operation, bridge.GetClusterStateResult).generation, 2)

    async def test_lost_reply_after_commit_and_accepted_cancellation(self):
        cluster = await self.make(piles=3)
        label = cluster.label("pile1")
        method = "/Ydb.Bridge.V1.BridgeService/UpdateClusterState"
        await self.control(cluster, "/failpoints", {
            "label": label, "method": method,
            "stage": "after_commit", "action": "unavailable"})
        req = bridge.UpdateClusterStateRequest()
        req.updates.add(pile_name="pile3", state=model.PUB_DISCONNECTED)
        with self.assertRaises(grpc.aio.AioRpcError) as caught:
            await cluster.bridge(label).UpdateClusterState(req)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertEqual((await self.state(cluster)).generation, 2)
        await self.control(cluster, "/failpoints", {
            "label": label, "method": method,
            "stage": "after_commit", "action": "release"})
        await self.control(cluster, "/failpoints", {"label": label, "method": method, "stage": "after_acceptance"})
        req.updates[0].pile_name = "pile2"
        pending = cluster.bridge(label).UpdateClusterState(req, timeout=0.1)
        with self.assertRaises(grpc.aio.AioRpcError):
            await pending
        self.assertEqual(cluster.inst("pile1").state.generation, 2)
        await self.control(cluster, "/failpoints", {
            "label": label, "method": method,
            "stage": "after_acceptance", "action": "release"})
        await wait_until(lambda: cluster.inst("pile1").state.generation == 3)

    async def test_lifecycle_authentication_and_self_management(self):
        cluster = await self.make(initial={"configured_piles": ["pile1"], "bridged": False},
                                  policies=Policies(admin_token="admin", read_token="reader"))
        stub = cluster.bridge(cluster.label("pile1"))
        self.assertEqual((await stub.ConvertToBridge(bridge.ConvertToBridgeRequest())).operation.status,
                         model.UNAUTHORIZED)
        self.assertEqual((await stub.GetClusterState(bridge.GetClusterStateRequest())).operation.status,
                         model.UNAUTHORIZED)
        response = await stub.ConvertToBridge(bridge.ConvertToBridgeRequest(), metadata=(("x-ydb-auth-ticket", "admin"),))
        self.assertEqual(response.operation.status, model.SUCCESS)
        await self.control(cluster, "/topology", {"self_management": False})
        response = await stub.GetClusterState(bridge.GetClusterStateRequest(), metadata=(("x-ydb-auth-ticket", "reader"),))
        self.assertEqual(response.operation.status, model.UNSUPPORTED)
