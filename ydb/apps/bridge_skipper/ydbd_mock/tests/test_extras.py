"""N-pile presets, minority primary, a pile that dies during resync, TLS."""
import asyncio
import os
import shutil
import subprocess
import tempfile
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, wait_until
from ydb.public.api.protos import ydb_discovery_pb2
from ydb.public.api.protos.draft import ydb_bridge_pb2

DISC, PRIMARY, SYNC, NOTSYNC = (model.PUB_DISCONNECTED, model.PUB_PRIMARY, model.PUB_SYNCHRONIZED,
                                model.PUB_NOT_SYNCHRONIZED)


class ExtraCase(unittest.IsolatedAsyncioTestCase):
    async def make(self, **kw) -> LiveCluster:
        c = LiveCluster(**kw)
        self.addAsyncCleanup(c.stop)
        await c.start()
        return c

    async def update(self, c, label, *pairs, quorum=()):
        req = ydb_bridge_pb2.UpdateClusterStateRequest()
        for name, state in pairs:
            req.updates.add(pile_name=name, state=state)
        req.quorum_piles.extend(quorum)
        return (await c.bridge(label).UpdateClusterState(req)).operation

    def stored(self, c, pile, of):
        return c.inst(pile).replica.state.stored(c.topo.pile_index(of)).name


class NPileTest(ExtraCase):
    async def test_four_piles_agree_on_every_commit(self):
        c = await self.make(piles=4, nodes=2)
        op = await self.update(c, c.label("pile2"), ("pile4", DISC))
        self.assertEqual(op.status, model.SUCCESS)
        await wait_until(lambda: all(c.inst(p).state.generation == 2 for p in c.topo.pile_names),
                         message="all four piles at generation 2")
        for pile in c.topo.pile_names:
            self.assertEqual(self.stored(c, pile, "pile4"), "DISCONNECTED")

    async def test_three_piles_two_disconnects_in_one_request_succeed(self):
        c = await self.make(piles=3)
        op = await self.update(c, c.label("pile1"), ("pile2", DISC), ("pile3", DISC))
        self.assertEqual(op.status, model.SUCCESS)
        self.assertEqual(c.inst("pile1").state.generation, 2)
        self.assertEqual(self.stored(c, "pile1", "pile2"), "DISCONNECTED")
        self.assertEqual(self.stored(c, "pile1", "pile3"), "DISCONNECTED")

    async def test_three_piles_one_dead_pile_is_failed_over_by_the_other_two(self):
        c = await self.make(piles=3)
        await c.kill("pile3")
        op = await self.update(c, c.label("pile1"), ("pile3", DISC))
        self.assertEqual(op.status, model.SUCCESS, operations.issue_texts(op))
        await wait_until(lambda: c.inst("pile2").state.generation == 2, message="pile2 follows")


class MinorityTest(ExtraCase):
    async def test_a_primary_alone_in_three_piles_cannot_commit(self):
        c = await self.make(piles=3)
        await c.kill("pile2")
        await c.kill("pile3")
        op = await self.update(c, c.label("pile1"), ("pile3", DISC))
        self.assertEqual(op.status, model.INTERNAL_ERROR)
        self.assertIn("no config quorum", operations.issue_texts(op)[0])
        self.assertEqual(c.inst("pile1").state.generation, 1, "a minority must not move the state")
        self.assertTrue(self.events_of(c, "pile1", "propose.fail"))

    async def test_without_config_quorum_enforcement_the_proposer_decides_alone(self):
        from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies
        c = await self.make(piles=3, policies=Policies(enforce_config_quorum=False))
        await c.kill("pile2")
        await c.kill("pile3")
        op = await self.update(c, c.label("pile1"), ("pile3", DISC))
        self.assertEqual(op.status, model.SUCCESS, operations.issue_texts(op))
        self.assertEqual(c.inst("pile1").state.generation, 2)

    def events_of(self, c, pile, kind):
        return c.inst(pile).events.query(kind=kind)


class DeadDuringResyncTest(ExtraCase):
    async def test_pile_that_dies_while_not_synchronized_can_be_failed_over_again(self):
        from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import tiny_timings
        c = await self.make(piles=3, timings=tiny_timings(sync_step_delay=0.4))
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile3", DISC))).status, model.SUCCESS)
        self.assertEqual((await self.update(c, c.label("pile1"), ("pile3", NOTSYNC))).status, model.SUCCESS)
        await wait_until(lambda: self.stored(c, "pile1", "pile3") == "NOT_SYNCHRONIZED_2", message="pile3 at _2")
        await c.kill("pile3")
        # the resync can neither finish nor wedge the cluster
        await asyncio.sleep(1.0)
        self.assertEqual(self.stored(c, "pile1", "pile3") in ("NOT_SYNCHRONIZED_2", "SYNCHRONIZED"), True)
        if self.stored(c, "pile1", "pile3") != "SYNCHRONIZED":
            op = await self.update(c, c.label("pile1"), ("pile3", DISC))
            self.assertEqual(op.status, model.SUCCESS, operations.issue_texts(op))
            self.assertEqual(self.stored(c, "pile1", "pile3"), "DISCONNECTED")


@unittest.skipUnless(shutil.which("openssl"), "needs the openssl command to make a certificate")
class TlsTest(ExtraCase):
    def make_cert(self, directory):
        cert, key = os.path.join(directory, "cert.pem"), os.path.join(directory, "key.pem")
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert,
            "-days", "2", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
            check=True, capture_output=True)
        with open(cert, "rb") as f:
            return cert, key, f.read()

    async def test_secure_listener_and_ssl_flag_in_discovery(self):
        with tempfile.TemporaryDirectory() as d:
            cert, key, pem = self.make_cert(d)
            c = await self.make(tls={"cert_file": cert, "key_file": key}, root_certificates=pem)
            label = c.label("pile1")
            resp = await c.discovery(label).ListEndpoints(ydb_discovery_pb2.ListEndpointsRequest())
            result = operations.unpack(resp.operation, ydb_discovery_pb2.ListEndpointsResult)
            self.assertTrue(result.endpoints[0].ssl)
            got = await c.bridge(label).GetClusterState(ydb_bridge_pb2.GetClusterStateRequest())
            self.assertEqual(got.operation.status, model.SUCCESS)
            # a plaintext client is not served
            import grpc
            plain = grpc.aio.insecure_channel(c.topo.node_by_label(label).endpoint)
            from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc
            with self.assertRaises(grpc.aio.AioRpcError):
                await ydb_bridge_v1_pb2_grpc.BridgeServiceStub(plain).GetClusterState(
                    ydb_bridge_pb2.GetClusterStateRequest(), timeout=2.0)
            await plain.close()

    async def test_missing_certificate_is_a_clear_error(self):
        from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import TopologyError
        c = LiveCluster(tls={"cert_file": "/nonexistent/cert.pem", "key_file": "/nonexistent/key.pem"})
        with self.assertRaises((TopologyError, OSError)):
            await c.start()
        await c.stop()


if __name__ == "__main__":
    unittest.main()
