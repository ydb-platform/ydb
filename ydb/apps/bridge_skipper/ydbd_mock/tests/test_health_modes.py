"""BSC placement and fallback evidence."""
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import health, model
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings
from ydb.public.api.protos import ydb_monitoring_pb2 as M


class HealthModesTest(unittest.IsolatedAsyncioTestCase):
    async def make(self):
        c = LiveCluster(piles=3, timings=tiny_timings(promote_delay=60))
        self.addAsyncCleanup(c.stop)
        await c.start()
        return c

    def build(self, c, pile):
        inst = c.inst(pile)
        return health.build(inst, inst.pile_info.nodes[0])

    def parts(self, result):
        return {i.location.storage.pool.group.pile.name: i for i in result.issue_log
                if i.type in ("BRIDGE_GROUP", "STORAGE_GROUP") and
                i.location.storage.pool.group.HasField("pile")}

    async def test_silent_primary_loss_is_fallback_grey_without_bridge_parts(self):
        c = await self.make()
        c.inst("pile2").set_blocked([0])
        result = self.build(c, "pile2")
        self.assertEqual(result.self_check_result, M.SelfCheck.EMERGENCY)
        self.assertTrue(any(i.type == "STORAGE" and i.status == M.StatusFlag.RED and
                            i.message == "System tablet BSC didn't provide information" for i in result.issue_log))
        self.assertFalse(any(i.type == "BRIDGE_GROUP" for i in result.issue_log))
        part = self.parts(result)["pile1"]
        self.assertEqual((part.type, part.status, part.message),
                         ("STORAGE_GROUP", M.StatusFlag.GREY, "Group has disks with no data"))

    async def test_fallback_reply_also_carries_pile_issues(self):
        c = await self.make()
        c.inst("pile2").set_blocked([0])
        result = self.build(c, "pile2")
        self.assertEqual((result.issue_log[0].type, result.issue_log[0].message),
                         ("STORAGE", "System tablet BSC didn't provide information"))
        part = self.parts(result)["pile1"]
        self.assertEqual((part.type, part.status), ("STORAGE_GROUP", M.StatusFlag.GREY))
        piles = [i for i in result.issue_log if i.type.startswith("PILE_")]
        self.assertEqual(sorted(i.type for i in piles), ["PILE_BOARD", "PILE_SCHEME_BOARD", "PILE_STATE_STORAGE"])
        for issue in piles:
            self.assertEqual((issue.status, issue.message, issue.location.compute.state_storage.pile.name),
                             (M.StatusFlag.RED, "There is not enough functional rings", "pile1"))

    async def test_fallback_refused_nodes_are_red_and_silent_nodes_are_grey(self):
        c = await self.make()
        c.inst("pile2").set_blocked([0])
        for f in c.inst("pile3").node_faults:
            f.down = True
        await c.inst("pile2").heartbeat_once()
        result = self.build(c, "pile2")
        part = self.parts(result)["pile3"]
        self.assertEqual((part.type, part.status), ("STORAGE_GROUP", M.StatusFlag.RED))
        reasons = {i.id: i for i in result.issue_log if i.id in part.reason}
        self.assertEqual(len(reasons), 1)
        self.assertEqual([(i.type, i.status) for i in reasons.values()], [("VDISK", M.StatusFlag.RED)])
        for f in c.inst("pile3").node_faults:
            f.down = False
            f.silence = True
        await c.inst("pile2").heartbeat_once()
        part = self.parts(self.build(c, "pile2"))["pile3"]
        self.assertEqual(part.status, M.StatusFlag.GREY)

    async def test_fallback_ignores_client_only_unavailable_nodes(self):
        c = await self.make()
        c.inst("pile2").set_blocked([0])
        for f in c.inst("pile3").node_faults:
            f.unavailable = True
        await c.inst("pile2").heartbeat_once()
        result = self.build(c, "pile2")
        self.assertTrue(any(i.type == "STORAGE" for i in result.issue_log), "the responder is in fallback")
        self.assertNotIn("pile3", self.parts(result), "a keeper-only fault is not whiteboard evidence")

    async def test_bsc_host_requires_its_static_quorum(self):
        c = await self.make()
        for f in c.inst("pile1").node_faults[:2]:
            f.down = True
        await c.inst("pile2").heartbeat_once()
        result = self.build(c, "pile2")
        self.assertTrue(any(i.type == "STORAGE" for i in result.issue_log))
        self.assertFalse(any(i.type == "BRIDGE_GROUP" for i in result.issue_log))

    async def test_method_scoped_silence_only_changes_fallback_for_selfcheck(self):
        c = await self.make()
        source = c.inst("pile2")
        source.set_blocked([0])
        for methods, no_data in (
                (["UpdateClusterState"], False),
                (["GetClusterState"], False),
                (["SelfCheck"], True),
                (["/Ydb.Monitoring.V1.MonitoringService/SelfCheck"], True),
                (None, True)):
            with self.subTest(methods=methods):
                status, body = await c.http("pile3", "POST", "/control", {
                    "all": True, "op": "silence", "on": True, "methods": methods})
                self.assertEqual(status, 200, body)
                await source.heartbeat_once()
                part = self.parts(self.build(c, "pile2")).get("pile3")
                if no_data:
                    self.assertIsNotNone(part)
                    self.assertEqual(part.status, M.StatusFlag.GREY)
                else:
                    self.assertIsNone(part)

    async def test_one_silent_node_preserves_no_data_despite_static_quorum(self):
        c = await self.make()
        source = c.inst("pile2")
        source.set_blocked([0])
        c.inst("pile3").node_faults[0].silence = True
        await source.heartbeat_once()
        self.assertTrue(source.has_static_quorum(2))
        part = self.parts(self.build(c, "pile2"))["pile3"]
        self.assertEqual((part.status, part.message), (M.StatusFlag.GREY, "Group has disks with no data"))

    async def test_bsc_partition_blames_from_the_bsc_host_perspective(self):
        c = await self.make()
        c.inst("pile1").set_blocked([2])
        c.inst("pile3").set_blocked([0])
        await c.inst("pile2").heartbeat_once()
        for pile in ("pile1", "pile2"):
            part = self.parts(self.build(c, pile))["pile3"]
            self.assertEqual((part.type, part.status), ("BRIDGE_GROUP", M.StatusFlag.RED))
        self.assertFalse(any(i.type == "BRIDGE_GROUP" for i in self.build(c, "pile3").issue_log))

    async def test_promoted_pile_hosts_bsc_before_followup(self):
        c = await self.make()
        for inst in (c.inst(p) for p in c.topo.pile_names):
            inst.override_replica({"pile2": model.PUB_PROMOTED})
        c.inst("pile1").set_blocked([1])
        self.assertTrue(any(i.type == "STORAGE" for i in self.build(c, "pile1").issue_log))
        c.inst("pile2").set_blocked([0])
        self.assertEqual(self.parts(self.build(c, "pile2"))["pile1"].type, "BRIDGE_GROUP")
