"""Deterministic transport faults, cluster Lab API and TLS client authentication."""
import asyncio
import os
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import grpc

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
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
