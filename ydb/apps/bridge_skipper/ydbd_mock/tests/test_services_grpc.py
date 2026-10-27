"""Real gRPC and real HTTP peers, in one event loop, on free ports."""
import asyncio
import tempfile
import time
import unittest

import grpc
import yaml
from google.protobuf import json_format

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings, wait_until
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies
from ydb.public.api.protos import ydb_bridge_common_pb2
from ydb.public.api.protos import ydb_config_pb2
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos import ydb_monitoring_pb2
from ydb.public.api.protos.draft import ydb_bridge_pb2

PRIMARY, PROMOTED, SYNC, NOTSYNC, SUSP, DISC = (
    model.PUB_PRIMARY, model.PUB_PROMOTED, model.PUB_SYNCHRONIZED, model.PUB_NOT_SYNCHRONIZED,
    model.PUB_SUSPENDED, model.PUB_DISCONNECTED)


def update_request(*pairs, quorum=()):
    req = ydb_bridge_pb2.UpdateClusterStateRequest()
    for name, state in pairs:
        req.updates.add(pile_name=name, state=state)
    req.quorum_piles.extend(quorum)
    return req


class GrpcCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._clusters = []

    async def asyncTearDown(self):
        for c in self._clusters:
            await c.stop()

    async def make(self, **kw) -> LiveCluster:
        c = LiveCluster(**kw)
        self._clusters.append(c)
        await c.start()
        return c

    async def get_state(self, c, label):
        resp = await c.bridge(label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest())
        self.assertEqual(resp.operation.status, model.SUCCESS)
        return operations.unpack(resp.operation, ydb_bridge_pb2.GetClusterStateResult)

    async def update(self, c, label, *pairs, quorum=(), metadata=None):
        resp = await c.bridge(label).UpdateClusterState(update_request(*pairs, quorum=quorum), metadata=metadata)
        return resp.operation

    async def control(self, c, pile, **body):
        status, out = await c.http(pile, "POST", "/control", body)
        self.assertEqual(status, 200, out)
        return out

    def events(self, c, pile, **kw):
        return c.inst(pile).events.query(**kw)


class SuccessEnvelopeTest(GrpcCase):
    async def test_empty_pile_name_survives_reads_control_and_mutations(self):
        c = await self.make(pile_names=["named", ""])
        label = c.label("named")
        state = await self.get_state(c, label)
        self.assertEqual([(p.pile_name, p.state) for p in state.pile_states], [("named", PRIMARY), ("", SYNC)])
        response = await c.discovery(c.label("")).ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest())
        discovery = operations.unpack(response.operation, ydb_discovery_pb2.ListEndpointsResult)
        self.assertEqual(discovery.endpoints[0].bridge_pile_name, "")
        self.assertEqual([p.pile_name for p in discovery.pile_states], ["named", ""])

        await self.control(c, "named", label=label, op="storage", piles=[""])
        response = await c.monitoring(label).SelfCheck(ydb_monitoring_pb2.SelfCheckRequest())
        health = operations.unpack(response.operation, ydb_monitoring_pb2.SelfCheckResult)
        self.assertEqual(len(health.issue_log), 1)
        self.assertTrue(health.issue_log[0].location.storage.pool.group.HasField("pile"))
        self.assertEqual(health.issue_log[0].location.storage.pool.group.pile.name, "")
        op = await self.update(c, label, ("", DISC))
        self.assertEqual(op.status, model.SUCCESS)
        response = await c.bridge(label).RemovePile(ydb_bridge_pb2.RemovePileRequest(pile_name=""))
        self.assertEqual(response.operation.status, model.SUCCESS)
        state = await self.get_state(c, label)
        self.assertEqual([(p.pile_name, p.state) for p in state.pile_states],
                         [("named", PRIMARY), ("", model.PUB_INACTIVE)])
        status, body = await c.http("", "GET", "/state")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["pile"], "")

    async def test_list_endpoints_returns_only_the_answering_node(self):
        c = await self.make()
        for pile in ("pile1", "pile2"):
            for n in (1, 2, 3):
                label = c.label(pile, n)
                node = c.topo.node_by_label(label)
                resp = await c.discovery(label).ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest(database=""))
                op = resp.operation
                self.assertTrue(op.ready)
                self.assertEqual(op.status, model.SUCCESS)
                result = operations.unpack(op, ydb_discovery_pb2.ListEndpointsResult)
                self.assertEqual(len(result.endpoints), 1)
                ep = result.endpoints[0]
                self.assertEqual((ep.address, ep.port, ep.node_id, ep.bridge_pile_name, ep.ssl),
                                 (node.host, node.grpc_port, node.node_id, pile, False))
                self.assertEqual(ep.location, pile)
                self.assertEqual(result.self_location, pile)
                self.assertEqual([(s.pile_name, s.state) for s in result.pile_states],
                                 [("pile1", PRIMARY), ("pile2", SYNC)])

    async def test_fetch_config_returns_exactly_one_entry_with_valid_yaml(self):
        c = await self.make(piles=2, nodes=3)
        for mode_req in (ydb_config_pb2.FetchConfigRequest(),
                         ydb_config_pb2.FetchConfigRequest(mode=ydb_config_pb2.FetchConfigRequest.ALL)
                         if hasattr(ydb_config_pb2.FetchConfigRequest, "ALL") else ydb_config_pb2.FetchConfigRequest()):
            resp = await c.config(c.label("pile2")).FetchConfig(mode_req)
            self.assertEqual(resp.operation.status, model.SUCCESS)
            result = operations.unpack(resp.operation, ydb_config_pb2.FetchConfigResult)
            self.assertEqual(len(result.config), 1)
            entry = result.config[0]
            self.assertEqual(entry.identity.WhichOneof("type"), "main")
            self.assertEqual(entry.identity.cluster, "mock-bridge")
            doc = yaml.safe_load(entry.config)
            self.assertEqual(doc["metadata"]["kind"], "MainConfig")
            hosts = doc["config"]["hosts"]
            self.assertEqual([h["host"] for h in hosts],
                             ["mock-pile1-1", "mock-pile1-2", "mock-pile1-3",
                              "mock-pile2-1", "mock-pile2-2", "mock-pile2-3"])
            self.assertEqual([h["location"]["bridge_pile_name"] for h in hosts], ["pile1"] * 3 + ["pile2"] * 3)

    async def test_self_check_is_good_and_names_the_node(self):
        c = await self.make()
        resp = await c.monitoring(c.label("pile2", 2)).SelfCheck(ydb_monitoring_pb2.SelfCheckRequest(
            return_verbose_status=True, do_not_cache=True))
        result = operations.unpack(resp.operation, ydb_monitoring_pb2.SelfCheckResult)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.GOOD)
        self.assertEqual(len(result.issue_log), 0)
        self.assertEqual((result.location.id, result.location.pile.name), (5, "pile2"))

    async def test_get_cluster_state_initial(self):
        c = await self.make()
        for label in (c.label("pile1"), c.label("pile2", 3)):
            got = await self.get_state(c, label)
            self.assertEqual(got.generation, 1)
            self.assertEqual([(s.pile_name, s.state) for s in got.pile_states], [("pile1", PRIMARY), ("pile2", SYNC)])


class BridgeFlowTest(GrpcCase):
    async def test_failover_on_one_instance_is_seen_on_the_other(self):
        c = await self.make()
        op = await self.update(c, c.label("pile1"), ("pile2", DISC))
        self.assertEqual(op.status, model.SUCCESS)
        self.assertTrue(op.ready)
        operations.unpack(op, ydb_bridge_pb2.UpdateClusterStateResult)
        await wait_until(lambda: self._gen(c, c.label("pile1", 2), 2), message="generation 2 on pile1 node 2")
        await wait_until(lambda: self._replica_gen(c, "pile2", 2), message="generation 2 on pile2")
        # pile2 stopped serving gRPC when it learnt it is DISCONNECTED: ask its replica over the control API
        await wait_until(lambda: self._replica_vector(c, "pile2", [PRIMARY, DISC]), message="pile2 replica")
        got = await self.get_state(c, c.label("pile1"))
        self.assertEqual([(s.pile_name, s.state) for s in got.pile_states], [("pile1", PRIMARY), ("pile2", DISC)])

    async def _replica_vector(self, c, pile, want):
        state = await c.state(pile)
        return [model.public_value(p["public"]) for p in state["piles"]] == want

    async def _replica_gen(self, c, pile, want):
        return (await c.state(pile))["generation"] == want

    async def _gen(self, c, label, want):
        return (await self.get_state(c, label)).generation == want

    async def test_validation_errors_are_ready_operations_with_a_status(self):
        c = await self.make()
        cases = [
            ([], model.BAD_REQUEST, "Updates list cannot be empty"),
            ([("nope", DISC)], model.BAD_REQUEST, "Invalid pile name# nope"),
            ([("pile1", DISC)], model.BAD_REQUEST, "Request must result in a state with one primary pile"),
            ([("pile2", NOTSYNC)], model.INTERNAL_ERROR,
             "Invalid transition from SYNCHRONIZED to NOT_SYNCHRONIZED_1"),
            ([("pile2", ydb_bridge_common_pb2.PileState.UNSPECIFIED)], model.INTERNAL_ERROR,
             "Unsupported pile state"),
            ([("pile2", getattr(ydb_bridge_common_pb2.PileState, "INACTIVE", 7))],
             model.BAD_REQUEST if model.PUB_INACTIVE is not None else model.INTERNAL_ERROR,
             "Pile can't be switched to INACTIVE state, it has to be removed from the cluster instead"
             if model.PUB_INACTIVE is not None else "Unsupported pile state"),
        ]
        for pairs, status, message in cases:
            op = await self.update(c, c.label("pile1"), *pairs)
            self.assertTrue(op.ready)
            self.assertEqual(op.status, status, message)
            self.assertEqual(operations.issue_texts(op), [message])
        self.assertEqual((await self.get_state(c, c.label("pile1"))).generation, 1)
        self.assertEqual([e["message"] for e in self.events(c, "pile1", kind="reject")][-1], cases[-1][2])

    async def test_takedown_of_the_primary_is_rejected_like_the_real_server(self):
        c = await self.make()
        op = await self.update(c, c.label("pile1"), ("pile1", SUSP), ("pile2", PROMOTED))
        self.assertEqual(op.status, model.BAD_REQUEST)
        self.assertEqual(operations.issue_texts(op), ["Request must result in a state with one primary pile"])

    async def test_switchover_commits_and_the_promote_follow_up_follows(self):
        c = await self.make()
        op = await self.update(c, c.label("pile1"), ("pile2", PROMOTED))
        self.assertEqual(op.status, model.SUCCESS)

        async def promoted():
            got = await self.get_state(c, c.label("pile2"))
            return got.generation == 3 and got.pile_states[1].state == PRIMARY

        await wait_until(promoted, message="promote follow-up")
        got = await self.get_state(c, c.label("pile1"))
        self.assertEqual([s.state for s in got.pile_states], [SYNC, PRIMARY])

    async def test_no_config_quorum_error_envelope(self):
        c = await self.make(piles=3)
        await c.kill("pile3")
        op = await self.update(c, c.label("pile1"), ("pile3", SUSP))
        self.assertEqual(op.status, model.INTERNAL_ERROR)
        self.assertEqual(operations.issue_texts(op), ["mock: no config quorum for piles [pile3]"])


class UnimplementedTest(GrpcCase):
    async def test_unimplemented_method_of_a_registered_service(self):
        c = await self.make()
        label = c.label("pile1")
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await c.discovery(label).WhoAmI(ydb_discovery_pb2.WhoAmIRequest())
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNIMPLEMENTED)
        found = self.events(c, "pile1", kind="rpc.unimplemented")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["method"], "/Ydb.Discovery.V1.DiscoveryService/WhoAmI")
        self.assertTrue(found[0]["registered"])
        self.assertEqual(found[0]["node"], label)

    async def test_other_registered_services_have_unimplemented_methods_too(self):
        from ydb.public.api.protos import ydb_config_pb2 as cfg
        c = await self.make()
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await c.config(c.label("pile1")).ReplaceConfig(cfg.ReplaceConfigRequest())
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNIMPLEMENTED)
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await c.monitoring(c.label("pile1")).NodeCheck(ydb_monitoring_pb2.NodeCheckRequest())
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNIMPLEMENTED)
        methods = [e["method"] for e in self.events(c, "pile1", kind="rpc.unimplemented")]
        self.assertEqual(methods, ["/Ydb.Config.V1.ConfigService/ReplaceConfig",
                                   "/Ydb.Monitoring.V1.MonitoringService/NodeCheck"])

    async def test_service_that_is_not_registered(self):
        c = await self.make()
        label = c.label("pile2", 2)
        call = c.channel(label).unary_unary("/Ydb.Table.V1.TableService/CreateSession")
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await call(b"")
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNIMPLEMENTED)
        found = self.events(c, "pile2", kind="rpc.unimplemented")
        self.assertEqual(found[-1]["method"], "/Ydb.Table.V1.TableService/CreateSession")
        self.assertFalse(found[-1]["registered"])


class RpcEventsTest(GrpcCase):
    async def test_rpc_start_and_end_are_recorded_per_node(self):
        c = await self.make()
        label = c.label("pile1", 2)
        await self.get_state(c, label)
        await self.update(c, c.label("pile1"), ("pile2", DISC))
        starts = self.events(c, "pile1", kind="rpc.start")
        ends = self.events(c, "pile1", kind="rpc.end")
        self.assertEqual([e["method"] for e in starts],
                         ["/Ydb.Bridge.V1.BridgeService/GetClusterState",
                          "/Ydb.Bridge.V1.BridgeService/UpdateClusterState"])
        self.assertEqual(starts[0]["node"], label)
        self.assertIn("pile2=DISCONNECTED", starts[1]["summary"])
        self.assertEqual([e["status"] for e in ends], ["SUCCESS", "SUCCESS"])
        self.assertTrue(all("duration_ms" in e for e in ends))

    async def test_error_status_is_in_the_end_event(self):
        c = await self.make()
        await self.update(c, c.label("pile1"), ("nope", DISC))
        self.assertEqual(self.events(c, "pile1", kind="rpc.end")[-1]["status"], "BAD_REQUEST")


class FaultTest(GrpcCase):
    async def test_unavailable_aborts_with_grpc_unavailable_and_clears(self):
        c = await self.make()
        label = c.label("pile1")
        await self.control(c, "pile1", label=label, op="unavailable", on=True)
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await self.get_state(c, label)
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNAVAILABLE)
        # another node of the same pile is untouched
        self.assertEqual((await self.get_state(c, c.label("pile1", 2))).generation, 1)
        await self.control(c, "pile1", label=label, op="clear")
        self.assertEqual((await self.get_state(c, label)).generation, 1)

    async def test_delay_applies_to_the_listed_methods_only(self):
        c = await self.make()
        label = c.label("pile1")
        await self.control(c, "pile1", label=label, op="delay", seconds=0.6, methods=["GetClusterState"])
        started = time.monotonic()
        await self.get_state(c, label)
        self.assertGreaterEqual(time.monotonic() - started, 0.58)
        started = time.monotonic()
        resp = await c.discovery(label).ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest())
        self.assertEqual(resp.operation.status, model.SUCCESS)
        self.assertLess(time.monotonic() - started, 0.5, "not delayed by the fault on another method")

    async def test_silence_until_the_client_gives_up(self):
        c = await self.make()
        label = c.label("pile2")
        await self.control(c, "pile2", label=label, op="silence", on=True)
        started = time.monotonic()
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await c.bridge(label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=0.4)
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertGreaterEqual(time.monotonic() - started, 0.35)
        await wait_until(lambda: self.events(c, "pile2", kind="rpc.cancelled"), message="rpc.cancelled")
        self.assertEqual(self.events(c, "pile2", kind="rpc.cancelled")[0]["method"],
                         "/Ydb.Bridge.V1.BridgeService/GetClusterState")
        await self.control(c, "pile2", label=label, op="clear")
        self.assertEqual((await self.get_state(c, label)).generation, 1)

    async def test_silence_applies_to_the_listed_methods_only(self):
        c = await self.make()
        label = c.label("pile1")
        state = await self.control(c, "pile1", label=label, op="silence", on=True, methods=["GetClusterState"])
        self.assertEqual(self.faults_of(state, label), {"silence": True, "silence_methods": ["GetClusterState"]})
        resp = await c.discovery(label).ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest(), timeout=1.0)
        self.assertEqual(resp.operation.status, model.SUCCESS)
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await c.bridge(label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=0.3)
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        state = await self.control(c, "pile1", label=label, op="silence", on=False)
        self.assertEqual(self.faults_of(state, label), {})

    def faults_of(self, state, label):
        return next(n["faults"] for n in state["nodes"] if n["label"] == label)

    async def test_silence_is_released_by_clear(self):
        c = await self.make()
        label = c.label("pile1")
        await self.control(c, "pile1", label=label, op="silence", on=True)
        pending = asyncio.ensure_future(self.get_state(c, label))
        await asyncio.sleep(0.3)
        self.assertFalse(pending.done())
        await self.control(c, "pile1", label=label, op="silence", on=False)
        got = await asyncio.wait_for(pending, 2.0)
        self.assertEqual(got.generation, 1)

    async def test_silence_max_clears_itself(self):
        c = await self.make()
        label = c.label("pile1")
        await self.control(c, "pile1", label=label, op="silence", on=True, max=0.4)
        started = time.monotonic()
        got = await self.get_state(c, label)
        elapsed = time.monotonic() - started
        self.assertEqual(got.generation, 1)
        self.assertGreaterEqual(elapsed, 0.35)
        self.assertLess(elapsed, 2.0)
        state = await c.state("pile1")
        self.assertNotIn("silence", state["nodes"][0]["faults"])

    async def test_down_closes_the_listener_and_up_reopens_it(self):
        c = await self.make()
        label = c.label("pile1", 3)
        await self.control(c, "pile1", label=label, op="down", on=True)
        started = time.monotonic()
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await self.get_state(c, label)
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertLess(time.monotonic() - started, 2.0, "a closed listener refuses at once")
        self.assertTrue(c.inst("pile1").ready(), "one node of three down: the pile is still ready")
        await self.control(c, "pile1", label=label, op="down", on=False)

        async def back():
            try:
                return (await self.get_state(c, label)).generation == 1
            except grpc.aio.AioRpcError:
                return False

        await wait_until(back, timeout=8.0, message="node reachable again")

    async def test_silence_cancelled_by_down(self):
        c = await self.make()
        label = c.label("pile1", 2)
        await self.control(c, "pile1", label=label, op="silence", on=True)
        pending = asyncio.ensure_future(self.get_state(c, label))
        await asyncio.sleep(0.2)
        await self.control(c, "pile1", label=label, op="down", on=True)
        with self.assertRaises(grpc.aio.AioRpcError):
            await asyncio.wait_for(pending, 3.0)

    async def test_unavailable_on_every_node_does_not_break_the_peer_links(self):
        c = await self.make()
        await self.control(c, "pile1", all=True, op="unavailable", on=True)
        for n in (1, 2, 3):
            with self.assertRaises(grpc.aio.AioRpcError):
                await self.get_state(c, c.label("pile1", n))
        await asyncio.sleep(0.8)
        self.assertTrue(c.inst("pile2").connected(0), "keeper_split must not be visible to the cluster")
        op = await self.update(c, c.label("pile2"), ("pile1", PRIMARY), ("pile2", SYNC))
        self.assertEqual(op.status, model.SUCCESS)


class HealthOverGrpcTest(GrpcCase):
    async def test_failed_resyncing_pile_is_not_reported_as_a_bridge_group_failure(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, True)
        self.assertTrue(model.requires_data_quorum(state.per_pile[1]))
        data = await self.self_check(c, c.label("pile1"))
        self.assertEqual(data["self_check_result"], "GOOD")
        self.assertFalse(data.get("issue_log"))
        self.assertEqual(data["database_status"][0]["storage"]["pools"][0]["groups"][0]["overall"], "GREEN")

    async def self_check(self, c, label):
        resp = await c.monitoring(label).SelfCheck(ydb_monitoring_pb2.SelfCheckRequest())
        result = operations.unpack(resp.operation, ydb_monitoring_pb2.SelfCheckResult)
        return json_format.MessageToDict(result, preserving_proto_field_name=True)

    def demo_filter(self, issue):
        """Mirrors filter_pile_issue in the demo's health.py."""
        try:
            return bool(issue["location"]["storage"]["pool"]["group"]["pile"]["name"]
                        and "GROUP" in issue["type"] and issue["status"] == "RED")
        except KeyError:
            return False

    async def test_manual_storage_blame_is_visible_to_the_demo_filter(self):
        c = await self.make()
        await self.control(c, "pile1", label=c.label("pile1"), op="storage", piles=["pile2"])
        data = await self.self_check(c, c.label("pile1"))
        self.assertEqual(data["self_check_result"], "MAINTENANCE_REQUIRED")
        issues = [i for i in data["issue_log"] if self.demo_filter(i)]
        self.assertEqual([i["location"]["storage"]["pool"]["group"]["pile"]["name"] for i in issues], ["pile2"])
        self.assertIn("storage", issues[0]["message"])
        # a node without the fault still says GOOD
        data = await self.self_check(c, c.label("pile1", 2))
        self.assertEqual(data["self_check_result"], "GOOD")

    async def test_killed_pile_is_blamed_by_the_survivor_until_it_is_failed_over(self):
        c = await self.make()
        await c.kill("pile2")
        label = c.label("pile1")

        async def blamed():
            data = await self.self_check(c, label)
            return [i for i in data.get("issue_log", []) if self.demo_filter(i)]

        await wait_until(blamed, message="auto blame of the dead pile")
        data = await self.self_check(c, label)
        self.assertEqual(data["self_check_result"], "MAINTENANCE_REQUIRED")
        op = await self.update(c, label, ("pile2", DISC))
        self.assertEqual(op.status, model.SUCCESS)

        async def clean():
            data = await self.self_check(c, label)
            return data["self_check_result"] == "GOOD" and not data.get("issue_log")

        await wait_until(clean, message="blame disappears after failover")

    async def test_partition_makes_both_sides_blame_each_other_while_staying_responsive(self):
        c = await self.make()
        for pile, other in (("pile1", "pile2"), ("pile2", "pile1")):
            status, _ = await c.http(pile, "POST", "/link", {"block": [other]})
            self.assertEqual(status, 200)

        async def mutual():
            a = await self.self_check(c, c.label("pile1"))
            b = await self.self_check(c, c.label("pile2"))
            return any(self.demo_filter(i) for i in a.get("issue_log", [])) and \
                any(self.demo_filter(i) for i in b.get("issue_log", []))

        await wait_until(mutual, message="mutual blame")

    async def test_auto_blame_can_be_switched_off(self):
        c = await self.make(policies=Policies(auto_blame_dead_piles=False))
        await c.kill("pile2")
        await wait_until(lambda: not c.inst("pile1").connected(1), message="pile2 seen down")
        data = await self.self_check(c, c.label("pile1"))
        self.assertEqual(data["self_check_result"], "GOOD")

    async def test_self_check_override(self):
        c = await self.make()
        label = c.label("pile1")
        await self.control(c, "pile1", label=label, op="self_check", value="DEGRADED")
        self.assertEqual((await self.self_check(c, label))["self_check_result"], "DEGRADED")
        await self.control(c, "pile1", label=label, op="self_check", value=None)
        self.assertEqual((await self.self_check(c, label))["self_check_result"], "GOOD")


class AdminTokenTest(GrpcCase):
    async def test_update_needs_the_token_and_get_does_not(self):
        c = await self.make(policies=Policies(admin_token="secret"))
        label = c.label("pile1")
        op = await self.update(c, label, ("pile2", DISC))
        self.assertEqual(op.status, model.UNAUTHORIZED)
        self.assertEqual(operations.issue_texts(op), ["Bridge operations require administrator privileges"])
        op = await self.update(c, label, ("pile2", DISC), metadata=(("x-ydb-auth-ticket", "wrong"),))
        self.assertEqual(op.status, model.UNAUTHORIZED)
        self.assertEqual((await self.get_state(c, label)).generation, 1)
        op = await self.update(c, label, ("pile2", DISC), metadata=(("x-ydb-auth-ticket", "secret"),))
        self.assertEqual(op.status, model.SUCCESS)


class PersistenceTest(GrpcCase):
    async def test_revive_keeps_the_persisted_replica(self):
        with tempfile.TemporaryDirectory() as root:
            c = await self.make(state_root=root)
            op = await self.update(c, c.label("pile1"), ("pile2", DISC))
            self.assertEqual(op.status, model.SUCCESS)
            await wait_until(lambda: c.inst("pile2").state.generation == 2, message="push to pile2")
            await c.kill("pile1")
            proc = await c.revive("pile1")
            self.assertEqual(proc.inst.state.generation, 2)
            self.assertEqual((await self.get_state(c, c.label("pile1"))).generation, 2)


class DisconnectedPileStopsServingTest(GrpcCase):
    """run.cpp: a node whose own pile lacks the config-quorum trait stops its gRPC servers."""

    async def refused(self, c, label) -> bool:
        try:
            await c.bridge(label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=0.5)
            return False
        except grpc.aio.AioRpcError as e:
            return e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)

    async def test_a_disconnected_pile_goes_dark_and_the_other_keeps_serving(self):
        c = await self.make()
        op = await self.update(c, c.label("pile1"), ("pile2", DISC))
        self.assertEqual(op.status, model.SUCCESS)
        for n in (1, 2, 3):
            label = c.label("pile2", n)
            await wait_until(lambda label=label: self.refused(c, label), message="%s refuses calls" % label)
        self.assertEqual((await self.get_state(c, c.label("pile1"))).pile_states[1].state, DISC)
        # the peer API stays up: that is how the pile hears about its way back
        state = await c.state("pile2")
        self.assertEqual(state["piles"][1]["public"], "DISCONNECTED")

    async def test_the_reply_to_the_request_that_disconnects_the_node_is_delivered(self):
        c = await self.make()
        op = await self.update(c, c.label("pile2"), ("pile2", DISC))
        self.assertEqual(op.status, model.SUCCESS, op.issues)
        await wait_until(lambda: self.refused(c, c.label("pile2")), message="pile2 dark after the reply")

    async def test_disconnect_waits_for_a_slow_update_reply_before_stopping_listener(self):
        c = await self.make(timings=tiny_timings(peer_timeout=2.0))
        transport = c.procs["pile2"].transport
        original_call = transport.call

        async def delayed_commit(src, dst, op, payload, timeout):
            result = await original_call(src, dst, op, payload, timeout)
            if op == "commit":
                await asyncio.sleep(1.25)
            return result

        transport.call = delayed_commit
        op = await asyncio.wait_for(
            self.update(c, c.label("pile2"), ("pile2", DISC)), timeout=3.0)
        self.assertEqual(op.status, model.SUCCESS, op.issues)
        await wait_until(lambda: self.refused(c, c.label("pile2")), timeout=3.0,
                         message="disconnected listener stops after its update reply")

    async def test_disconnect_does_not_wait_for_a_silenced_read_rpc(self):
        c = await self.make()
        label = c.label("pile2")
        await self.control(c, "pile2", label=label, op="silence", on=True)
        pending = asyncio.create_task(self.get_state(c, label))
        await asyncio.sleep(0.1)
        op = await asyncio.wait_for(self.update(c, c.label("pile1"), ("pile2", DISC)), timeout=2.0)
        self.assertEqual(op.status, model.SUCCESS, op.issues)
        with self.assertRaises(grpc.aio.AioRpcError):
            await asyncio.wait_for(pending, timeout=3.0)
        await wait_until(lambda: self.refused(c, label), timeout=3.0, message="silent reader is drained")

    async def test_a_rejoining_pile_serves_again_once_it_is_not_synchronized_2(self):
        c = await self.make()
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile2", DISC))).status, model.SUCCESS)
        await wait_until(lambda: self.refused(c, c.label("pile2")), message="dark")
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile2", NOTSYNC))).status, model.SUCCESS)
        await wait_until(lambda: c.inst("pile2").state.stored(1) == model.Stored.NOT_SYNCHRONIZED_2,
                         message="connect follow-up reached pile2")
        await wait_until(lambda: self._serves(c, c.label("pile2")), message="pile2 serves again")

    async def _serves(self, c, label) -> bool:
        return not await self.refused(c, label)

    async def test_a_suspended_pile_keeps_serving(self):
        c = await self.make(piles=3, timings=tiny_timings(suspend_delay=60.0))
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile3", SUSP))).status, model.SUCCESS)
        await wait_until(lambda: c.inst("pile3").state.stored(2) == model.Stored.SUSPENDED, message="suspended")
        await asyncio.sleep(0.2)
        self.assertFalse(await self.refused(c, c.label("pile3")), "SUSPENDED has the config quorum trait")

    async def test_the_policy_can_keep_a_disconnected_pile_serving(self):
        c = await self.make(policies=Policies(close_grpc_when_disconnected=False))
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile2", DISC))).status, model.SUCCESS)
        await wait_until(lambda: c.inst("pile2").state.stored(1) == model.Stored.DISCONNECTED, message="installed")
        await asyncio.sleep(0.3)
        self.assertFalse(await self.refused(c, c.label("pile2")))

    async def test_a_node_fault_cleared_while_the_pile_is_disconnected_does_not_reopen_it(self):
        c = await self.make()
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile2", DISC))).status, model.SUCCESS)
        await wait_until(lambda: self.refused(c, c.label("pile2")), message="dark")
        await self.control(c, "pile2", all=True, op="down", on=True)
        await self.control(c, "pile2", all=True, op="down", on=False)
        await asyncio.sleep(0.2)
        self.assertTrue(await self.refused(c, c.label("pile2")))


if __name__ == "__main__":
    unittest.main()
