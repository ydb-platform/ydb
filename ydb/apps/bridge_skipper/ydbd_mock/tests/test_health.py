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
        self.assertIn("connectivity", result.issue_log[0].message)
        self.assertIn("storage", result.issue_log[1].message)
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

    async def test_both_resync_stages_are_invisible_even_when_they_hold_data_quorum(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.set_blocked([1])
        for stage in (model.Stored.NOT_SYNCHRONIZED_1, model.Stored.NOT_SYNCHRONIZED_2):
            with self.subTest(stage=stage):
                state = inst.state.copy()
                state.per_pile[1] = stage
                inst.replica.force(state, False)
                self.assertEqual(model.requires_data_quorum(stage), stage == model.Stored.NOT_SYNCHRONIZED_2)
                self.assertEqual(health.blame(inst, 0), {})
                result = self.build(c)
                self.assertEqual(len(result.issue_log), 0)
                self.assertEqual(result.database_status[0].storage.pools[0].groups[0].overall, Flag.GREEN)
        inst.override_replica({"pile2": model.PUB_SYNCHRONIZED})
        self.assertEqual(health.blame(inst, 0), {"pile2": health.CONNECTIVITY})

    async def test_manual_blame_obeys_the_synchronized_visibility_filter(self):
        c = await self.make()
        inst = c.inst("pile1")
        inst.node_faults[0].blame_storage = {"pile2"}
        for stored in model.Stored:
            with self.subTest(stored=stored):
                state = inst.state.copy()
                state.per_pile[1] = stored
                inst.replica.force(state, False)
                expected = {"pile2": health.STORAGE} if stored == model.Stored.SYNCHRONIZED else {}
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
        self.assertEqual(self.build(c).self_check_result, ydb_monitoring_pb2.SelfCheck.GOOD)

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
