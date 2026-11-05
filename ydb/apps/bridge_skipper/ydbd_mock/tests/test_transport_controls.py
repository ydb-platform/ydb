"""Deterministic transport faults, cluster Lab API and TLS client authentication."""
import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import aiohttp
import grpc
from aiohttp import web

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog
from ydb.apps.bridge_skipper.ydbd_mock.lib.faults import NodeFaults, NodeRuntime
from ydb.apps.bridge_skipper.ydbd_mock.lib.instance import _server_credentials
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Topology, TopologyError
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc
from ydb.public.api.protos.draft import ydb_bridge_pb2


METHOD = "/Ydb.Bridge.V1.BridgeService/GetClusterState"


class RuntimeControlsTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events = EventLog("pile1")
        self.rt = NodeRuntime(Topology().all_nodes()[0], NodeFaults(), self.events)

    async def test_hold_is_method_and_stage_specific_and_release_unblocks(self):
        self.rt.configure_failpoint(METHOD, "after_snapshot")
        await self.rt.checkpoint(METHOD, "after_commit", None)
        await self.rt.checkpoint("/Other.Service/GetClusterState", "after_snapshot", None)
        task = asyncio.create_task(self.rt.checkpoint(METHOD, "after_snapshot", None))
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.assertEqual(self.rt.failpoints_json()[0]["hits"], 1)
        self.rt.release_failpoint(METHOD, "after_snapshot")
        await asyncio.wait_for(task, 1)
        self.assertEqual(self.rt.failpoints_json(), [])
        self.assertEqual(self.events.kinds(), ["failpoint.hit", "failpoint.released"])

    async def test_cancellation_does_not_release_the_next_call(self):
        self.rt.configure_failpoint(METHOD, "after_snapshot")
        first = asyncio.create_task(self.rt.checkpoint(METHOD, "after_snapshot", None))
        await asyncio.sleep(0)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(self.rt.checkpoint(METHOD, "after_snapshot", None))
        await asyncio.sleep(0)
        self.assertFalse(second.done())
        self.rt.clear()
        await asyncio.wait_for(second, 1)
        self.assertEqual(self.rt.faults.to_json(), {})

    async def test_unavailable_is_an_explicit_grpc_abort(self):
        context = mock.Mock()
        context.abort = mock.AsyncMock(side_effect=RuntimeError("aborted"))
        self.rt.configure_failpoint(METHOD, "after_snapshot", "unavailable")
        with self.assertRaisesRegex(RuntimeError, "aborted"):
            await self.rt.checkpoint(METHOD, "after_snapshot", context)
        self.assertEqual(context.abort.call_args.args[0], grpc.StatusCode.UNAVAILABLE)

    async def test_one_shot_barrier_and_rpc_ids_disambiguate_overlapping_calls(self):
        self.rt.configure_failpoint(METHOD, "after_snapshot", once=True)

        async def call():
            await self.rt.checkpoint(METHOD, "after_snapshot", None)
            return "snapshot"

        first = asyncio.create_task(self.rt.run(METHOD, None, call))
        await asyncio.sleep(0)
        second = asyncio.create_task(self.rt.run(METHOD, None, call))
        self.assertEqual(await asyncio.wait_for(second, 1), "snapshot")
        self.assertFalse(first.done())
        self.rt.release_failpoint(METHOD, "after_snapshot")
        self.assertEqual(await asyncio.wait_for(first, 1), "snapshot")
        starts = self.events.query(kind="rpc.start")
        hit = self.events.query(kind="failpoint.hit")[0]
        released = self.events.query(kind="failpoint.released")[0]
        self.assertNotEqual(starts[0]["rpc_id"], starts[1]["rpc_id"])
        self.assertEqual(hit["rpc_id"], starts[0]["rpc_id"])
        self.assertEqual(released["rpc_id"], hit["rpc_id"])
        self.assertEqual({e["rpc_id"] for e in self.events.query(kind="rpc.end")},
                         {e["rpc_id"] for e in starts})

    def test_one_shot_full_response_override_is_consumed_once(self):
        response = {"operation": {"ready": True, "status": "PRECONDITION_FAILED"}}
        self.rt.set_rpc_override("UpdateClusterState", response, once=True)
        self.assertEqual(self.rt.take_rpc_override("UpdateClusterState"), response)
        self.assertIsNone(self.rt.take_rpc_override("UpdateClusterState"))
        self.assertTrue(self.events.query(kind="rpc.override")[0]["noncommitting"])

    def test_overrides_are_deep_copied_and_clearable(self):
        value = {"clusterState": {"generation": "9007199254740993"}}
        self.rt.set_response_override("GetClusterState", value)
        value["clusterState"]["generation"] = "2"
        got = self.rt.response_override("GetClusterState")
        self.assertEqual(got["clusterState"]["generation"], "9007199254740993")
        got["clusterState"]["generation"] = "3"
        self.assertEqual(self.rt.response_override("GetClusterState")["clusterState"]["generation"],
                         "9007199254740993")
        self.rt.set_response_override("GetClusterState", None)
        self.assertIsNone(self.rt.response_override("GetClusterState"))
        with self.assertRaises(ValueError):
            self.rt.set_response_override("UpdateClusterState", {})


class SupervisorHttpTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        Topology().save(os.path.join(self.tmp.name, "topology.yaml"))
        with open(os.path.join(self.tmp.name, "cluster.json"), "w") as f:
            json.dump({"cluster_id": "cluster-test"}, f)
        self.cluster = supervisor.Cluster(self.tmp.name)
        self.cluster.live_piles = lambda: ["pile1", "pile2"]
        self.cluster.alive = lambda pile: True
        self.calls = []

        def request(pile, method, path, body=None, timeout=5):
            self.calls.append((pile, method, path, body))
            return {"pile": pile, "generation": 7}

        self.cluster.request = request
        self.runner = web.AppRunner(supervisor.build_http_app(self.cluster, "supervisor-test"), access_log=None)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.url = "http://127.0.0.1:%d/api/v1" % self.runner.addresses[0][1]
        self.client = aiohttp.ClientSession()
        self.addAsyncCleanup(self.client.close)

    async def post(self, path, body, status=200, **kw):
        async with self.client.post(self.url + path, json=body, **kw) as response:
            self.assertEqual(response.status, status, await response.text())
            return await response.json()

    async def test_manifest_matches_the_direct_sdk_inventory(self):
        async with self.client.get(self.url + "/manifest") as response:
            data = await response.json()
        self.assertEqual(data["schema_version"], 1)
        self.assertEqual(data["kind"], "ydbd_mock")
        self.assertEqual(data["cluster_id"], "cluster-test")
        self.assertEqual(data["supervisor_id"], "supervisor-test")
        self.assertEqual(len(data["nodes"]), 6)
        self.assertEqual(data["endpoint_map"][data["nodes"][0]["label"]], data["nodes"][0]["dial_endpoint"])
        self.assertEqual(self.cluster.manifest()["topology_sha256"], data["topology_sha256"])

    async def test_node_faults_and_overrides_are_forwarded_only_to_their_pile(self):
        await self.post("/fault", {"target": "mock-pile2-2", "op": "unavailable", "on": True})
        self.assertEqual(self.calls, [("pile2", "POST", "/control",
                                      {"label": "mock-pile2-2", "op": "unavailable", "on": True})])
        await self.post("/response-override", {"target": "pile1", "method": "SelfCheck", "result": {}})
        self.assertEqual(self.calls[-1], (
            "pile1", "POST", "/response-override", {"all": True, "method": "SelfCheck", "result": {}}))

    async def test_topology_is_submitted_once_but_followup_holds_are_broadcast(self):
        await self.post("/topology", {"configured_piles": ["pile1"], "bridged": False})
        self.assertEqual(self.calls, [("pile1", "POST", "/topology",
                                      {"configured_piles": ["pile1"], "bridged": False})])
        self.calls.clear()
        await self.post("/followups", {"hold": ["promote"]})
        self.assertEqual(self.calls, [
            (p, "POST", "/followups", {"hold": ["promote"]}) for p in ("pile1", "pile2")])

    async def test_unknown_process_action_and_arbitrary_proxy_are_rejected(self):
        await self.post("/process", {"pile": "pile1", "action": "shell"}, status=400)
        await self.post("/fault", {"target": "pile1", "url": "http://other"}, status=400)
        async with self.client.post(self.url + "/peer/commit", json={}) as response:
            self.assertEqual(response.status, 404)
        self.assertEqual(self.calls, [])

    async def test_cross_origin_and_rebinding_host_are_rejected(self):
        await self.post("/reset", {}, status=403, headers={"Origin": "https://other.example"})
        await self.post("/reset", {}, status=403, headers={"Host": "other.example"})
        self.assertEqual(self.calls, [])

    async def test_partial_broadcast_reports_each_failure(self):
        def request(pile, *args, **kw):
            if pile == "pile2":
                raise supervisor.SupervisorError("unresponsive")
            return {"ok": True}

        self.cluster.request = request
        out = await self.post("/timings", {"values": {"promote_delay": 0}}, status=207)
        self.assertFalse(out["ok"])
        self.assertEqual(out["errors"], {"pile2": "unresponsive"})
        self.assertEqual(out["instances"], {"pile1": {"ok": True}})

    async def test_dead_pile_is_an_explicit_broadcast_failure(self):
        self.cluster.alive = lambda pile: pile == "pile1"
        out = await self.post("/followups", {"hold": ["promote"]}, status=207)
        self.assertFalse(out["ok"])
        self.assertIn("pile2", out["errors"])
        self.assertEqual(set(out["instances"]), {"pile1"})

    async def test_aggregate_state_preserves_unresponsive_and_dead_piles(self):
        self.cluster.alive = lambda pile: pile == "pile1"
        self.cluster.request = mock.Mock(side_effect=supervisor.SupervisorError("stopped"))
        async with self.client.get(self.url + "/state") as response:
            out = await response.json()
        self.assertEqual(set(out["instances"]), {"pile1", "pile2"})
        self.assertTrue(out["instances"]["pile1"]["alive"])
        self.assertFalse(out["instances"]["pile1"]["responsive"])
        self.assertFalse(out["instances"]["pile2"]["alive"])

    async def test_partition_validation_happens_before_any_mutation(self):
        await self.post("/partition", {"spec": "pile1|missing"}, status=400)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.cluster._macros(), [])

    async def test_partial_partition_reports_outcomes_and_retains_undo_record(self):
        def request(pile, method, path, body=None, **kw):
            if pile == "pile2":
                raise supervisor.SupervisorError("lost link reply")
            return {"blocked": ["pile2"]}

        self.cluster.request = request
        out = await self.post("/partition", {"spec": "pile1|pile2"}, status=207)
        self.assertFalse(out["ok"])
        self.assertTrue(out["uncertain"])
        self.assertEqual((out["action"], out["stage"]), ("partition", "links"))
        self.assertEqual(out["results"]["links"], {"pile1": {"blocked": ["pile2"]}})
        self.assertEqual(out["errors"]["links"], {"pile2": "lost link reply"})
        self.assertEqual(self.cluster._macros(), [{"kind": "partition", "spec": "pile1|pile2"}])

    async def test_partial_heal_reports_outcomes_and_keeps_records_for_retry(self):
        macros = [{"kind": "partition", "spec": "pile1|pile2"}]
        self.cluster._save_macros(macros)

        def request(pile, method, path, body=None, **kw):
            if pile == "pile2":
                raise supervisor.SupervisorError("heal reply lost")
            return {"blocked": []}

        self.cluster.request = request
        out = await self.post("/heal", {}, status=207)
        self.assertEqual((out["action"], out["stage"]), ("heal", "links"))
        self.assertEqual(out["attempted"]["links"], ["pile1", "pile2"])
        self.assertEqual(out["results"]["links"], {"pile1": {"blocked": []}})
        self.assertEqual(out["errors"]["links"], {"pile2": "heal reply lost"})
        self.assertEqual(self.cluster._macros(), macros)

    async def test_partial_reset_includes_install_and_cleanup_failures(self):
        macros = [{"kind": "partition", "spec": "pile1|pile2"}]
        self.cluster._save_macros(macros)

        def request(pile, method, path, body=None, **kw):
            self.calls.append((pile, method, path, body))
            if path == "/reset-local" and pile == "pile2":
                raise supervisor.SupervisorError("commit reply lost")
            if path == "/reset-cancel" and pile == "pile1":
                raise supervisor.SupervisorError("resume reply lost")
            if path == "/state":
                return {"generation": 7, "max_generation": 9}
            return {"ok": True}

        self.cluster.request = request
        with mock.patch.object(supervisor, "_signal", return_value=None):
            out = await self.post("/reset", {}, status=207)
        self.assertEqual((out["action"], out["stage"], out["generation"]), ("reset", "install", 10))
        self.assertEqual(out["results"]["install"], {"pile1": {"ok": True}})
        self.assertEqual(out["errors"]["install"], {"pile2": "commit reply lost"})
        self.assertEqual(out["errors"]["resume"], {"pile1": "resume reply lost"})
        self.assertEqual(out["attempted"]["resume"], ["pile1", "pile2"])
        self.assertEqual(self.cluster._macros(), macros)

    async def test_failed_reset_prepare_resumes_even_the_pile_with_a_lost_reply(self):
        def request(pile, method, path, body=None, **kw):
            if path == "/reset-prepare" and pile == "pile2":
                raise supervisor.SupervisorError("prepare reply lost")
            return {"ok": True}

        self.cluster.request = request
        with mock.patch.object(supervisor, "_signal", return_value=None):
            out = await self.post("/reset", {}, status=207)
        self.assertEqual(out["stage"], "prepare")
        self.assertEqual(out["attempted"]["resume"], ["pile1", "pile2"])
        self.assertNotIn("install", out["attempted"])


class SupervisorEventsTest(unittest.TestCase):
    def test_event_pagination_does_not_skip_a_pile_when_its_clock_moves_back(self):
        cluster = mock.Mock()
        cluster.piles.return_value = ["pile1"]
        events = [{"seq": seq, "ts": ts, "pile": "pile1", "kind": "rpc.start"}
                  for seq, ts in ((1, 10), (2, 9), (3, 11))]
        with mock.patch.object(supervisor, "read_events", return_value=events):
            first = supervisor._http_events(cluster, {}, 1, None)
            second = supervisor._http_events(cluster, first["cursors"], 10, None)
        self.assertEqual([e["seq"] for e in first["events"] + second["events"]], [1, 2, 3])
        self.assertTrue(first["has_more"])
        self.assertFalse(second["has_more"])


class TlsConfigurationTest(unittest.TestCase):
    def test_invalid_client_auth_configuration_is_rejected(self):
        for extra in ({"require_client_auth": True}, {"require_client_auth": "true"}, {"root_cert_file": ""}):
            with self.assertRaises(TopologyError):
                Topology(tls={"cert_file": "cert", "key_file": "key", **extra})

    def test_server_credentials_include_trust_roots_and_client_auth(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = {}
            for name, content in (("cert_file", b"cert"), ("key_file", b"key"), ("root_cert_file", b"root")):
                paths[name] = os.path.join(directory, name)
                with open(paths[name], "wb") as f:
                    f.write(content)
            with mock.patch("grpc.ssl_server_credentials") as credentials:
                _server_credentials(dict(paths, require_client_auth=True))
            credentials.assert_called_once_with([(b"key", b"cert")], root_certificates=b"root", require_client_auth=True)


@unittest.skipUnless(shutil.which("openssl"), "needs openssl to generate test certificates")
class MutualTlsTest(unittest.IsolatedAsyncioTestCase):
    def certificate(self, directory, name, common_name="localhost"):
        cert = os.path.join(directory, name + ".pem")
        key = os.path.join(directory, name + ".key")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                        "-out", cert, "-days", "2", "-subj", "/CN=" + common_name, "-addext",
                        "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, capture_output=True)
        with open(cert, "rb") as f:
            pem = f.read()
        with open(key, "rb") as f:
            key_pem = f.read()
        return cert, key, pem, key_pem

    async def test_trusted_client_succeeds_and_missing_or_untrusted_credentials_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key, trusted, trusted_key = self.certificate(directory, "trusted")
            _, _, foreign, foreign_key = self.certificate(directory, "foreign")
            cluster = LiveCluster(piles=1, nodes=1, tls={"cert_file": cert, "key_file": key,
                                  "root_cert_file": cert, "require_client_auth": True})
            try:
                await cluster.start()
                endpoint = cluster.topo.all_nodes()[0].endpoint
                cases = [(trusted, trusted_key, trusted, True), (trusted, None, None, False),
                         (trusted, foreign_key, foreign, False), (foreign, trusted_key, trusted, False)]
                for roots, client_key, client_cert, success in cases:
                    with self.subTest(trusted_roots=roots == trusted, client=client_key is not None, success=success):
                        credentials = grpc.ssl_channel_credentials(roots, client_key, client_cert)
                        channel = grpc.aio.secure_channel(endpoint, credentials)
                        try:
                            stub = ydb_bridge_v1_pb2_grpc.BridgeServiceStub(channel)
                            if success:
                                response = await stub.GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=2)
                                self.assertTrue(response.operation.ready)
                            else:
                                with self.assertRaises(grpc.aio.AioRpcError):
                                    await stub.GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=1)
                        finally:
                            await channel.close()
            finally:
                await cluster.stop()

    async def test_trusted_certificate_requires_admin_cn_or_token_for_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            cert, key, admin_cert, admin_key = self.certificate(directory, "admin", "admin")
            _, _, user_cert, user_key = self.certificate(directory, "user", "user")
            roots = os.path.join(directory, "client-roots.pem")
            with open(roots, "wb") as f:
                f.write(admin_cert + user_cert)
            policies = Policies(admin_certificate_cn=["admin"], admin_token="fallback")
            cluster = LiveCluster(piles=1, nodes=1, policies=policies, tls={"cert_file": cert, "key_file": key,
                                  "root_cert_file": roots, "require_client_auth": True})
            channels = []
            try:
                await cluster.start()
                endpoint = cluster.topo.all_nodes()[0].endpoint
                for client_cert, client_key, admin in ((admin_cert, admin_key, True), (user_cert, user_key, False)):
                    channel = grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials(
                        admin_cert, client_key, client_cert))
                    channels.append(channel)
                    stub = ydb_bridge_v1_pb2_grpc.BridgeServiceStub(channel)
                    read = await stub.GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=2)
                    self.assertEqual(read.operation.status, model.SUCCESS)
                    request = ydb_bridge_pb2.UpdateClusterStateRequest()
                    request.updates.add(pile_name="pile1", state=model.PUB_PRIMARY)
                    response = await stub.UpdateClusterState(request, timeout=2)
                    self.assertEqual(response.operation.status, model.SUCCESS if admin else model.UNAUTHORIZED)
                    if not admin:
                        response = await stub.UpdateClusterState(request, metadata=[("x-ydb-auth-ticket", "fallback")],
                                                                 timeout=2)
                        self.assertEqual(response.operation.status, model.SUCCESS)
                cluster.inst("pile1").policies.read_token = "read-secret"
                admin_stub = ydb_bridge_v1_pb2_grpc.BridgeServiceStub(channels[0])
                denied = await admin_stub.GetClusterState(ydb_bridge_pb2.GetClusterStateRequest(), timeout=2)
                self.assertEqual(denied.operation.status, model.UNAUTHORIZED)
                allowed = await admin_stub.GetClusterState(
                    ydb_bridge_pb2.GetClusterStateRequest(),
                    metadata=[("x-ydb-auth-ticket", "read-secret")], timeout=2)
                self.assertEqual(allowed.operation.status, model.SUCCESS)
            finally:
                for channel in channels:
                    await channel.close()
                await cluster.stop()
