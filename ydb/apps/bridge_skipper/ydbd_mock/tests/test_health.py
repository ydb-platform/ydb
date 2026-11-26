"""SelfCheck building: manual and automatic blame, the two issue styles, overrides."""
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import health, model
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies
from ydb.public.api.protos import ydb_monitoring_pb2

Flag = ydb_monitoring_pb2.StatusFlag


class HealthCase(unittest.IsolatedAsyncioTestCase):
    async def make(self, **kw) -> LiveCluster:
        c = LiveCluster(**kw)
        self.addAsyncCleanup(c.stop)
        await c.start()
        return c

    def build(self, c, pile="pile1", n=1):
        inst = c.inst(pile)
        return health.build(inst, inst.pile_info.nodes[n - 1])


class MinimalStyleTest(HealthCase):
    async def test_healthy_node_has_no_issues(self):
        c = await self.make()
        result = self.build(c)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.GOOD)
        self.assertEqual(len(result.issue_log), 0)
        self.assertEqual((result.location.id, result.location.host, result.location.pile.name),
                         (1, "mock-pile1-1", "pile1"))

    async def test_manual_blame_gives_one_red_bridge_group_issue_per_pile(self):
        c = await self.make(piles=3)
        faults = c.inst("pile1").node_faults[0]
        faults.blame_storage = {"pile3"}
        faults.blame_connectivity = {"pile2"}
        result = self.build(c)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.MAINTENANCE_REQUIRED)
        piles = [i.location.storage.pool.group.pile.name for i in result.issue_log]
        self.assertEqual(piles, ["pile2", "pile3"])
        for issue in result.issue_log:
            self.assertEqual((issue.type, issue.status, issue.level), ("BRIDGE_GROUP", Flag.RED, 5))
            self.assertEqual(issue.message, "Group failed")
        self.assertEqual(len({i.id for i in result.issue_log}), 2)

    async def test_blame_is_per_node(self):
        c = await self.make()
        c.inst("pile1").node_faults[1].blame_storage = {"pile2"}
        self.assertEqual(len(self.build(c, n=1).issue_log), 0)
        self.assertEqual(len(self.build(c, n=2).issue_log), 1)
        self.assertEqual(len(self.build(c, n=3).issue_log), 0)

    async def test_manual_blame_wins_over_automatic_for_the_same_pile(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.node_faults[0].blame_storage = {"pile2"}
        inst.set_blocked([1])
        self.assertEqual(health.blame(inst, 0), {"pile2": health.STORAGE})
        self.assertEqual(health.blame(inst, 1), {"pile2": health.CONNECTIVITY})

    async def test_override_beats_the_computed_verdict(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.node_faults[0].self_check = "MAINTENANCE_REQUIRED"
        inst.node_faults[0].blame_storage = {"pile2"}
        result = self.build(c)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.MAINTENANCE_REQUIRED)
        self.assertEqual(len(result.issue_log), 1)


class AutomaticBlameTest(HealthCase):
    async def test_unreachable_synchronized_pile_is_blamed(self):
        c = await self.make()
        inst = c.inst("pile1")
        self.assertEqual(health.blame(inst, 0), {})
        inst.set_blocked([1])
        self.assertEqual(health.blame(inst, 0), {"pile2": health.CONNECTIVITY})

    async def test_disconnected_pile_is_not_blamed(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        inst.override_replica({"pile2": model.PUB_DISCONNECTED})
        self.assertEqual(health.blame(inst, 0), {})

    async def test_suspended_pile_is_not_blamed_it_does_not_hold_data_quorum(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        inst.override_replica({"pile2": model.PUB_SUSPENDED})
        self.assertEqual(inst.replica.state.per_pile[1], model.Stored.SUSPENDED)
        self.assertEqual(health.blame(inst, 0), {})

    async def test_stage1_pile_is_invisible_without_data_quorum(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_1
        inst.replica.force(state, False)
        self.assertEqual(health.blame(inst, 0), {})
        result = self.build(c)
        self.assertEqual(len(result.issue_log), 0)
        self.assertEqual(result.database_status[0].storage.pools[0].groups[0].overall, Flag.GREEN)

    async def test_stage2_pile_is_visible_with_data_quorum(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        self.assertEqual(health.blame(inst, 0), {"pile2": health.CONNECTIVITY})
        result = self.build(c)
        self.assertEqual(len(result.issue_log), 1)
        self.assertEqual(result.database_status[0].storage.pools[0].groups[0].overall, Flag.ORANGE)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.MAINTENANCE_REQUIRED)

    async def test_manual_blame_obeys_the_data_quorum_visibility_filter(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.node_faults[0].blame_storage = {"pile2"}
        for stored in model.Stored:
            with self.subTest(stored=stored):
                state = inst.state.copy()
                state.per_pile[1] = stored
                inst.replica.force(state, False)
                expected = {"pile2": health.STORAGE} if model.requires_data_quorum(stored) else {}
                self.assertEqual(health.blame(inst, 0), expected)

    async def test_node_never_blames_its_own_pile(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.reach.blocked = {0}
        self.assertNotIn("pile1", health.blame(inst, 0))

    async def test_policy_off_means_no_automatic_blame(self):
        c = await self.make(policies=Policies(auto_blame_dead_piles=False))
        inst = c.inst("pile1")
        inst.set_blocked([1])
        self.assertEqual(health.blame(inst, 0), {})
        # The blocked pile still yields RED PILE_* issues, so only the absence of parts is checked.
        self.assertFalse(any(i.type == "BRIDGE_GROUP" for i in self.build(c).issue_log))

    async def test_pile_without_static_quorum_has_state_storage_issues(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        result = self.build(c)
        piles = [i for i in result.issue_log if i.type.startswith("PILE_")]
        self.assertEqual(len(piles), 3)
        self.assertEqual([i.type for i in piles], ["PILE_STATE_STORAGE", "PILE_SCHEME_BOARD", "PILE_BOARD"])
        for issue in piles:
            self.assertEqual((issue.status, issue.message, issue.location.compute.state_storage.pile.name),
                             (Flag.RED, "There is not enough functional rings", "pile2"))
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.EMERGENCY)
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        self.assertFalse(any(i.type.startswith("PILE_") for i in self.build(c).issue_log))
        state.per_pile[1] = model.Stored.SYNCHRONIZED
        inst.replica.force(state, False)
        inst.set_blocked([])
        c.inst("pile2").node_faults[0].down = True
        await inst.heartbeat_once()
        result = self.build(c)
        piles = [i for i in result.issue_log if i.type.startswith("PILE_")]
        self.assertEqual([(i.status, i.message, i.location.compute.state_storage.pile.name) for i in piles],
                         [(Flag.BLUE, "One ring has unavailable replicas", "pile2")] * 3)
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.DEGRADED)

    async def test_three_piles_each_blame_only_the_dead_one(self):
        c = await self.make(piles=3)
        inst = c.inst("pile1")
        inst.set_blocked([2])
        self.assertEqual(health.blame(inst, 0), {"pile3": health.CONNECTIVITY})
        self.assertEqual(health.blame(c.inst("pile2"), 0), {})


class RealisticStyleTest(HealthCase):
    async def test_group_aggregation_matches_ydb_for_partial_and_total_loss(self):
        c = await self.make(piles=3, policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        for blamed, flag, verdict in (
            (set(), Flag.GREEN, ydb_monitoring_pb2.SelfCheck.GOOD),
            ({"pile2", "pile3"}, Flag.ORANGE, ydb_monitoring_pb2.SelfCheck.MAINTENANCE_REQUIRED),
            ({"pile1", "pile2", "pile3"}, Flag.RED, ydb_monitoring_pb2.SelfCheck.EMERGENCY),
        ):
            with self.subTest(blamed=blamed):
                inst.node_faults[0].blame_storage = blamed
                result = self.build(c)
                self.assertEqual(result.self_check_result, verdict)
                database = result.database_status[0]
                self.assertEqual([database.overall, database.storage.overall,
                                  database.storage.pools[0].overall,
                                  database.storage.pools[0].groups[0].overall], [flag] * 4)
                if blamed:
                    parent = result.issue_log[-1]
                    self.assertEqual(parent.status, flag)
                    self.assertFalse(parent.location.storage.pool.group.HasField("pile"))
                    self.assertEqual(list(parent.location.storage.pool.group.id),
                                     [database.storage.pools[0].groups[0].id])

    async def test_parent_storage_group_lists_the_children(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        c.inst("pile1").node_faults[0].blame_storage = {"pile2"}
        result = self.build(c)
        self.assertEqual([i.type for i in result.issue_log], ["BRIDGE_GROUP", "STORAGE_GROUP"])
        child, parent = result.issue_log
        self.assertEqual(list(parent.reason), [child.id])
        self.assertTrue(parent.location.storage.pool.group.HasField("pile"))
        self.assertEqual(parent.location.storage.pool.group.pile.name, "pile2")
        self.assertEqual(list(parent.location.storage.pool.group.id),
                         [result.database_status[0].storage.pools[0].groups[0].id])
        self.assertNotEqual(list(parent.location.storage.pool.group.id),
                            list(child.location.storage.pool.group.id))
        self.assertLess(parent.level, child.level)
        self.assertEqual(parent.status, Flag.ORANGE, "pile1 is still alive, the group is dead in some piles only")

    async def test_parent_is_red_when_every_synchronized_pile_is_blamed(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        faults = c.inst("pile1").node_faults[0]
        faults.blame_storage = {"pile1", "pile2"}
        result = self.build(c)
        parent = result.issue_log[-1]
        self.assertEqual((parent.type, parent.status), ("STORAGE_GROUP", Flag.RED))
        self.assertEqual(sorted(parent.reason), sorted(i.id for i in result.issue_log[:-1]))
        self.assertFalse(parent.location.storage.pool.group.HasField("pile"))
        self.assertEqual(list(parent.location.storage.pool.group.id),
                         [result.database_status[0].storage.pools[0].groups[0].id])

    async def test_dead_stage2_pile_has_orange_parent_with_its_pile(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        inst.node_faults[0].blame_storage = {"pile2"}
        result = self.build(c)
        self.assertEqual([i.type for i in result.issue_log], ["BRIDGE_GROUP", "STORAGE_GROUP"])
        parent = result.issue_log[-1]
        self.assertEqual(parent.status, Flag.ORANGE)
        self.assertEqual(parent.location.storage.pool.group.pile.name, "pile2")

    async def test_healthy_stage2_pile_cannot_save_dead_synchronized_pile(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        inst.node_faults[0].blame_storage = {"pile1"}
        result = self.build(c)
        parent = result.issue_log[-1]
        self.assertEqual((parent.type, parent.status), ("STORAGE_GROUP", Flag.RED))
        # The stage-2 pile's replicating part is the second reason, so the parent names no pile.
        parts = [i for i in result.issue_log if i.type == "BRIDGE_GROUP"]
        self.assertEqual([i.location.storage.pool.group.pile.name for i in parts], ["pile1", "pile2"])
        self.assertEqual(list(parent.reason), [i.id for i in parts])
        self.assertFalse(parent.location.storage.pool.group.HasField("pile"))

    async def test_stage2_pile_replicates_with_blue_vdisk_reasons(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        result = self.build(c)
        self.assertEqual([i.type for i in result.issue_log], ["BRIDGE_GROUP", "VDISK", "STORAGE_GROUP"])
        part, vdisk, parent = result.issue_log
        self.assertEqual((part.status, part.message, part.location.storage.pool.group.pile.name),
                         (Flag.RED, "Group failed", "pile2"))
        self.assertEqual(list(part.reason), [vdisk.id])
        self.assertEqual((vdisk.status, vdisk.message), (Flag.BLUE, "Replication in progress"))
        self.assertEqual(parent.status, Flag.ORANGE)
        self.assertEqual(list(parent.reason), [part.id])
        self.assertEqual(result.self_check_result, ydb_monitoring_pb2.SelfCheck.MAINTENANCE_REQUIRED)
        self.assertEqual(result.database_status[0].storage.pools[0].groups[0].overall, Flag.ORANGE)
        inst.set_blocked([1])
        result = self.build(c)
        self.assertEqual([i.type for i in result.issue_log], ["BRIDGE_GROUP", "STORAGE_GROUP"])
        part = result.issue_log[0]
        self.assertEqual((part.status, part.location.storage.pool.group.pile.name), (Flag.RED, "pile2"))
        self.assertTrue(part.id.endswith("-connectivity"))
        self.assertEqual(list(part.reason), [])

    async def test_healthcheck_status_appends_the_size_issue_and_red_drops_vdisk_reasons(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        state = inst.state.copy()
        state.per_pile[1] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        verdict = self.build(c).self_check_result
        inst.node_faults[0].healthcheck_status = "YELLOW"
        result = self.build(c)
        last = result.issue_log[-1]
        self.assertEqual((last.type, last.status, last.level, last.message),
                         ("HEALTHCHECK_STATUS", Flag.YELLOW, 1, "Healthcheck response size exceeds 30 MB"))
        vdisks = [i.id for i in result.issue_log if i.type == "VDISK"]
        self.assertEqual(len(vdisks), 1)
        self.assertEqual(result.self_check_result, verdict)
        inst.node_faults[0].healthcheck_status = "RED"
        result = self.build(c)
        self.assertFalse(any(i.type == "VDISK" for i in result.issue_log))
        part = next(i for i in result.issue_log if i.type == "BRIDGE_GROUP")
        self.assertEqual(list(part.reason), vdisks)
        last = result.issue_log[-1]
        self.assertEqual((last.type, last.status, last.level, last.message),
                         ("HEALTHCHECK_STATUS", Flag.RED, 1, "Healthcheck response size exceeds 50 MB and will be truncated"))
        self.assertRegex(last.id, r"^RED-[0-9a-f]{4}$")
        self.assertEqual(result.self_check_result, verdict)

    async def test_dead_synchronized_piles_with_healthy_stage2_have_some_piles_message(self):
        c = await self.make(piles=3, policies=Policies(issue_style="realistic"))
        inst = c.inst("pile1")
        state = inst.state.copy()
        state.per_pile[2] = model.Stored.NOT_SYNCHRONIZED_2
        inst.replica.force(state, False)
        inst.node_faults[0].blame_storage = {"pile1", "pile2"}
        result = self.build(c)
        parent = result.issue_log[-1]
        self.assertEqual((parent.type, parent.status), ("STORAGE_GROUP", Flag.RED))
        self.assertFalse(parent.location.storage.pool.group.HasField("pile"))
        self.assertEqual(parent.message, "Group dead in some piles")

    async def test_single_child_parent_preserves_an_explicit_empty_pile(self):
        c = await self.make(pile_names=["pile1", ""], policies=Policies(issue_style="realistic"))
        c.inst("pile1").node_faults[0].blame_storage = {""}
        result = self.build(c)
        child, parent = result.issue_log
        self.assertEqual(list(parent.reason), [child.id])
        self.assertTrue(parent.location.storage.pool.group.HasField("pile"))
        self.assertEqual(parent.location.storage.pool.group.pile.name, "")
        self.assertEqual(list(parent.location.storage.pool.group.id),
                         [result.database_status[0].storage.pools[0].groups[0].id])

    async def test_no_parent_without_children(self):
        c = await self.make(policies=Policies(issue_style="realistic"))
        self.assertEqual(len(self.build(c).issue_log), 0)


if __name__ == "__main__":
    unittest.main()
