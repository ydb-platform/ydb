"""Several PileInstance objects in one event loop, over the in-memory network, with tiny delays."""
import asyncio
import json
import os
import random
import tempfile
import time
import unittest
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.cluster import PileInstance
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import Stored
from ydb.apps.bridge_skipper.ydbd_mock.lib.peers import InMemoryNetwork, PeerError
from ydb.apps.bridge_skipper.ydbd_mock.lib.replica import Prepared, ReplicaError
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Timings, Topology

PRIMARY, PROMOTED, SYNC, NOTSYNC, SUSP, DISC = (
    model.PUB_PRIMARY, model.PUB_PROMOTED, model.PUB_SYNCHRONIZED, model.PUB_NOT_SYNCHRONIZED,
    model.PUB_SUSPENDED, model.PUB_DISCONNECTED)
D, N1, N2, S, X = (Stored.DISCONNECTED, Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2,
                   Stored.SYNCHRONIZED, Stored.SUSPENDED)


def tiny_timings(**over):
    values = dict(
        promote_delay=0.03, suspend_delay=0.03, connect_delay=0.03, sync_step_delay=0.01,
        followup_stagger=0.005, followup_retry=0.05, quorum_fail_delay=0.6,
        heartbeat_interval=0.02, heartbeat_timeout=0.25, peer_timeout=0.15, prepare_ttl=0.3)
    values.update(over)
    return Timings(**values)


class Harness:
    def __init__(self, piles=3, timings=None, policies=None, state_root=None):
        names = ["pile%d" % (i + 1) for i in range(piles)]
        self.topo = Topology(names, 3, timings=timings or tiny_timings(), policies=policies or Policies())
        self.net = InMemoryNetwork()
        self.state_root = state_root
        self.instances = []

    def _make(self, i):
        state_dir = None
        if self.state_root:
            state_dir = os.path.join(self.state_root, self.topo.pile_names[i])
        inst = PileInstance(self.topo, i, self.net, state_dir=state_dir, rng=random.Random(i + 1))
        self.net.register(i, inst)
        return inst

    async def start(self):
        self.instances = [self._make(i) for i in range(len(self.topo.pile_names))]
        await asyncio.gather(*(i.start() for i in self.instances))
        return self

    async def stop(self):
        await asyncio.gather(*(i.stop() for i in self.instances))

    async def kill(self, i):
        self.net.kill(i)
        await self.instances[i].stop()

    async def restart(self, i):
        await self.instances[i].stop()
        self.net.revive(i)
        inst = self._make(i)
        self.instances[i] = inst
        await inst.start()
        return inst

    def gens(self):
        return [i.state.generation for i in self.instances]

    async def wait_push_failed(self, generation):
        """The background push to a dead pile has been attempted, so a restart cannot receive it."""
        def done():
            return any(
                e["kind"] == "push.failed" and e["generation"] == generation
                for inst in self.instances for e in inst.events.query(kind="push"))
        await wait_until(done, message="push of generation %d to the dead pile" % generation)

    def live(self):
        return [inst for k, inst in enumerate(self.instances) if k not in self.net.dead]


async def wait_until(predicate, timeout=4.0, step=0.01, message="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(step)
    raise AssertionError("timed out waiting for %s" % message)


class ReplicationCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.harnesses = []

    async def asyncTearDown(self):
        for h in self.harnesses:
            await h.stop()

    async def make(self, piles=3, persist=False, **kw):
        h = Harness(piles=piles, state_root=self._tmp.name if persist else None, **kw)
        self.harnesses.append(h)
        await h.start()
        return h

    def assertCommitsAreConsistent(self, h):
        """Every install at one generation, by any route (commit, push, pull), has the same content."""
        by_generation = {}
        for inst in h.instances:
            seen = []
            for e in inst.events.query(kind="commit"):
                if e["kind"] != "commit":
                    continue
                seen.append(e["generation"])
                by_generation.setdefault(e["generation"], set()).add(
                    (tuple(sorted(e["after"].items())), tuple(e["stored"])))
            self.assertEqual(seen, sorted(seen), "generations on %s went backwards: %s" % (inst.name, seen))
            self.assertEqual(len(seen), len(set(seen)), "duplicate commit on %s: %s" % (inst.name, seen))
        for generation, variants in by_generation.items():
            self.assertEqual(len(variants), 1, "two different commits at generation %d: %s" % (generation, variants))
        self.assertReplicasAgree(h)

    def assertReplicasAgree(self, h):
        """Two live replicas at the same generation hold the same snapshot (no split brain)."""
        by_generation = {}
        for inst in h.live():
            by_generation.setdefault(inst.state.generation, []).append(inst)
        for generation, group in by_generation.items():
            first = group[0].state.to_json()
            for inst in group[1:]:
                self.assertEqual(
                    inst.state.to_json(), first,
                    "split brain at generation %d: %s has %s, %s has %s" % (
                        generation, group[0].name, group[0].state.describe(group[0].names),
                        inst.name, inst.state.describe(inst.names)))


class AdminCommitTest(ReplicationCase):
    async def test_admin_commit_reaches_every_pile_with_the_same_generation(self):
        h = await self.make(3)
        outcome = await h.instances[0].admin_update([("pile3", DISC)])
        self.assertTrue(outcome.ok, outcome.error)
        await wait_until(lambda: h.gens() == [2, 2, 2], message="same generation everywhere")
        for inst in h.instances:
            self.assertEqual(inst.state.per_pile, [S, S, D])
            self.assertEqual(inst.state.sync, {2: 2})

    async def test_commit_can_start_on_any_pile(self):
        h = await self.make(3)
        outcome = await h.instances[2].admin_update([("pile2", DISC)])
        self.assertTrue(outcome.ok, outcome.error)
        await wait_until(lambda: h.gens() == [2, 2, 2])
        self.assertEqual([i.state.per_pile for i in h.instances], [[S, D, S]] * 3)

    async def test_validation_error_is_returned_and_logged(self):
        h = await self.make(2)
        outcome = await h.instances[0].admin_update([("pile1", DISC)])
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error.status, model.BAD_REQUEST)
        self.assertEqual(outcome.error.message, "Request must result in a state with one primary pile")
        self.assertEqual([e["message"] for e in h.instances[0].events.query(kind="reject")],
                         ["Request must result in a state with one primary pile"])
        self.assertEqual(h.gens(), [1, 1])

    async def test_noop_commit_still_bumps_the_generation(self):
        h = await self.make(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2])
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.instances[0].state.generation == 3)
        self.assertEqual(h.instances[0].state.per_pile, [S, D])

    async def test_quorum_piles_are_stored(self):
        h = await self.make(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)], ["pile1"])).ok)
        self.assertEqual(h.instances[0].state.quorum_piles_last, ["pile1"])


class FailoverQuorumTest(ReplicationCase):
    async def test_failover_of_a_dead_pile_commits_with_the_survivors_alone(self):
        h = await self.make(3)
        await h.kill(2)
        outcome = await h.instances[0].admin_update([("pile3", DISC)])
        self.assertTrue(outcome.ok, outcome.error)
        await wait_until(lambda: h.instances[0].state.generation == 2 and h.instances[1].state.generation == 2)
        self.assertEqual(h.instances[1].state.per_pile, [S, S, D])

    async def test_failover_of_the_dead_primary_to_a_new_primary(self):
        h = await self.make(2)
        await h.kill(0)
        outcome = await h.instances[1].admin_update([("pile1", DISC), ("pile2", PRIMARY)])
        self.assertTrue(outcome.ok, outcome.error)
        self.assertEqual(h.instances[1].state.primary, 1)
        self.assertEqual(h.instances[1].state.per_pile, [D, S])

    async def test_takedown_of_a_dead_pile_fails_with_no_config_quorum(self):
        h = await self.make(3, timings=tiny_timings(quorum_fail_delay=0.3))
        await h.kill(2)
        started = time.monotonic()
        outcome = await h.instances[0].admin_update([("pile3", SUSP)])
        elapsed = time.monotonic() - started
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error.status, model.INTERNAL_ERROR)
        self.assertEqual(outcome.error.message, "mock: no config quorum for piles [pile3]")
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertEqual(h.instances[0].state.generation, 1)
        self.assertIn("propose.fail", h.instances[0].events.kinds())
        # nothing is left prepared behind
        self.assertIsNone(h.instances[0].replica.prepared)
        self.assertIsNone(h.instances[1].replica.prepared)

    async def test_switchover_needs_every_synchronized_pile(self):
        h = await self.make(3, timings=tiny_timings(quorum_fail_delay=0.2))
        await h.kill(2)
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error.message, "mock: no config quorum for piles [pile3]")

    async def test_unenforced_quorum_commits_with_the_proposer_alone(self):
        h = await self.make(3, policies=Policies(enforce_config_quorum=False))
        await h.kill(2)
        await h.kill(1)
        outcome = await h.instances[0].admin_update([("pile3", SUSP)])
        self.assertTrue(outcome.ok, outcome.error)

    async def test_pile_without_a_node_majority_refuses_prepares(self):
        h = await self.make(2, timings=tiny_timings(quorum_fail_delay=0.2))
        h.instances[1].node_faults[0].down = True
        h.instances[1].node_faults[1].down = True
        h.instances[1].on_nodes_changed()
        self.assertFalse(h.instances[1].ready())
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error.message, "mock: no config quorum for piles [pile2]")
        reasons = [e["refusals"] for e in h.instances[0].events.query(kind="propose.retry")]
        self.assertTrue(any(r.get("pile2") in ("not_ready", "unreachable") for r in reasons), reasons)

    async def test_minority_of_nodes_down_keeps_the_pile_ready(self):
        h = await self.make(2)
        h.instances[1].node_faults[0].down = True
        h.instances[1].on_nodes_changed()
        self.assertTrue(h.instances[1].ready())
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertTrue(outcome.ok, outcome.error)


class StaleReplicaTest(ReplicationCase):
    async def test_disconnected_pile_is_not_pulled_to_and_rejoin_brings_it_back(self):
        h = await self.make(2, persist=True)
        await h.kill(1)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        self.assertEqual(h.instances[0].state.generation, 2)
        await wait_until(lambda: "push.failed" in h.instances[0].events.kinds(), message="push to the dead pile")

        inst2 = await h.restart(1)
        self.assertEqual(inst2.state.generation, 1)               # persisted, stale
        started = inst2.events.query(kind="start")[0]
        self.assertTrue(started["loaded"])
        await wait_until(
            lambda: any(e["kind"] == "pull.refused" for e in h.instances[0].events.query()) or
            h.instances[0].reach.last_generation.get(1) == 1,
            message="heartbeats exchanged")
        await asyncio.sleep(0.3)
        self.assertEqual(inst2.state.generation, 1, "a DISCONNECTED pile must not be pulled to")
        self.assertEqual(inst2.state.per_pile, [S, S])
        self.assertIn("pull.refused", inst2.events.kinds())

        # rejoin pushes to the returning pile, which jumps by more than one
        self.assertTrue((await h.instances[0].admin_update([("pile2", NOTSYNC)])).ok)
        await wait_until(lambda: inst2.state.generation >= 3, message="rejoin push")
        pushes = [e for e in inst2.events.query(kind="commit") if e["via"] == "push"]
        self.assertTrue(pushes)
        self.assertEqual(pushes[0]["generation"], 3)

        # then the automatic _1 -> _2 -> SYNCHRONIZED chain
        await wait_until(lambda: h.gens() == [5, 5], message="chain finished")
        for inst in h.instances:
            self.assertEqual(inst.state.per_pile, [S, S])
            self.assertEqual(inst.state.sync, {})
        causes = [e["cause"] for e in h.instances[0].events.query(kind="commit")]
        self.assertEqual(causes, ["admin", "admin", "connect", "synchronized"])
        self.assertCommitsAreConsistent(h)

    async def test_restart_reloads_and_catches_up_when_allowed(self):
        timings = tiny_timings(connect_delay=30.0)
        h = await self.make(3, persist=True, timings=timings)
        self.assertTrue((await h.instances[0].admin_update([("pile3", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2, 2])
        self.assertTrue((await h.instances[0].admin_update([("pile3", NOTSYNC)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3])
        self.assertEqual(h.instances[2].state.per_pile, [S, S, N1])

        await h.kill(2)
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertTrue(outcome.ok, outcome.error)        # pile3 is _1: not needed for quorum
        await wait_until(lambda: h.instances[0].state.generation == 5, message="promote follow-up")
        self.assertEqual(h.instances[0].state.primary, 1)
        await h.wait_push_failed(5)

        inst3 = await h.restart(2)
        self.assertEqual(inst3.events.query(kind="start")[0]["generation"], 3)
        await wait_until(lambda: inst3.state.generation == 5, message="catch up through anti-entropy")
        pulled = [e for e in inst3.events.query(kind="commit") if e["via"] == "pull"]
        self.assertEqual(len(pulled), 1)
        self.assertEqual(inst3.state.primary, 1)
        self.assertEqual(inst3.state.per_pile, [S, S, N1])

    async def test_anti_entropy_can_be_switched_off(self):
        timings = tiny_timings(connect_delay=30.0)
        h = await self.make(3, persist=True, timings=timings, policies=Policies(anti_entropy=False))
        self.assertTrue((await h.instances[0].admin_update([("pile3", DISC)])).ok)
        self.assertTrue((await h.instances[0].admin_update([("pile3", NOTSYNC)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3])
        await h.kill(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", PROMOTED)])).ok)
        await wait_until(lambda: h.instances[0].state.generation == 5, message="promote follow-up")
        await h.wait_push_failed(5)
        inst3 = await h.restart(2)
        await asyncio.sleep(0.4)
        self.assertEqual(inst3.state.generation, 3)

    async def test_persisted_replica_file_round_trips(self):
        h = await self.make(2, persist=True)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        path = os.path.join(self._tmp.name, "pile1", "replica.json")
        self.assertTrue(os.path.exists(path))
        await h.restart(0)
        self.assertEqual(h.instances[0].state.generation, 2)
        self.assertEqual(h.instances[0].state.per_pile, [S, D])


class DetachedReplicaTest(ReplicationCase):
    async def test_detached_replica_ignores_pushes_and_refuses_prepares(self):
        h = await self.make(2, timings=tiny_timings(quorum_fail_delay=0.2))
        h.instances[1].override_replica({}, detach=True)
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertFalse(outcome.ok)
        self.assertEqual(outcome.error.message, "mock: no config quorum for piles [pile2]")
        refusals = [e["refusals"] for e in h.instances[0].events.query(kind="propose.retry")]
        self.assertTrue(any(r.get("pile2") == "detached" for r in refusals), refusals)

        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await asyncio.sleep(0.2)
        self.assertEqual(h.instances[1].state.generation, 1)
        self.assertEqual(h.instances[0].state.generation, 2)

    async def test_detached_replica_does_not_run_followups_or_pull(self):
        h = await self.make(2)
        inst = h.instances[1]
        inst.override_replica({"pile1": PRIMARY, "pile2": PROMOTED}, generation=7, detach=True)
        await asyncio.sleep(0.2)
        self.assertEqual(inst.state.generation, 7)
        self.assertEqual(inst.pending_followups(), [])
        self.assertNotIn("followup.armed", inst.events.kinds())

    async def test_override_replica_with_an_older_generation(self):
        h = await self.make(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2])
        inst = h.instances[1]
        inst.override_replica({"pile2": PRIMARY, "pile1": SYNC}, generation=1, detach=True)
        self.assertEqual(inst.state.generation, 1)
        self.assertEqual(inst.state.primary, 1)
        await asyncio.sleep(0.2)
        self.assertEqual(inst.state.generation, 1)        # detached: nobody can pull it forward

    async def test_override_validates_input(self):
        h = await self.make(2)
        inst = h.instances[0]
        with self.assertRaises(KeyError):
            inst.override_replica({"nope": PRIMARY})
        with self.assertRaises(ValueError):
            inst.override_replica({"pile1": DISC})              # no primary left
        with self.assertRaises(ValueError):
            inst.override_replica({"pile2": PRIMARY})           # two primaries

    async def test_reattach_does_not_pull_a_disconnected_pile(self):
        h = await self.make(2)
        inst = h.instances[1]
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: inst.state.generation == 2, message="push of the failover commit")
        inst.override_replica({}, generation=1, detach=True)
        inst.clear_replica_override()
        await asyncio.sleep(0.2)
        self.assertEqual(inst.state.generation, 1)


class FollowupTest(ReplicationCase):
    async def test_switchover_then_promote_commit(self):
        h = await self.make(3)
        self.assertTrue((await h.instances[0].admin_update([("pile2", PROMOTED)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3], message="promote follow-up")
        for inst in h.instances:
            self.assertEqual((inst.state.primary, inst.state.promoted), (1, 1))
        causes = [e["cause"] for e in h.instances[2].events.query(kind="commit")]
        self.assertEqual(causes, ["admin", "promote"])
        self.assertCommitsAreConsistent(h)

    async def test_takedown_then_suspend_disconnects(self):
        h = await self.make(3)
        self.assertTrue((await h.instances[0].admin_update([("pile3", SUSP)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3], message="suspend -> disconnected")
        self.assertEqual(h.instances[0].state.per_pile, [S, S, D])
        self.assertEqual(h.instances[0].state.sync, {2: 2}, "the suspension generation, not the disconnect one")
        self.assertCommitsAreConsistent(h)

    async def test_followups_do_not_run_when_the_proposer_is_disconnected(self):
        h = await self.make(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2])
        self.assertEqual(h.instances[1].pending_followups(), [])

    async def test_racing_followups_produce_one_commit_per_generation(self):
        # Every instance arms the same promote follow-up. The generation CAS must leave exactly one commit.
        timings = tiny_timings(followup_stagger=0.0, promote_delay=0.05)
        h = await self.make(3, timings=timings)
        self.assertTrue((await h.instances[1].admin_update([("pile3", PROMOTED)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3], message="promote")
        await asyncio.sleep(0.3)
        self.assertEqual(h.gens(), [3, 3, 3])
        self.assertCommitsAreConsistent(h)

    async def test_connect_waits_for_the_pile_to_be_reachable(self):
        h = await self.make(2, persist=True)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2])
        await h.kill(1)
        self.assertTrue((await h.instances[0].admin_update([("pile2", NOTSYNC)])).ok)
        await asyncio.sleep(0.5)
        self.assertEqual(h.instances[0].state.per_pile, [S, N1], "no connect while pile2 is unreachable")
        await h.restart(1)
        await wait_until(lambda: h.instances[0].state.per_pile == [S, S], message="whole chain after revive")

    async def test_resync_progress_is_in_the_state_view(self):
        timings = tiny_timings(sync_step_delay=0.2)
        h = await self.make(2, timings=timings)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        self.assertTrue((await h.instances[0].admin_update([("pile2", NOTSYNC)])).ok)
        await wait_until(lambda: h.instances[0].state.per_pile == [S, N2], message="connected")
        view = h.instances[0].state_json()["piles"][1]
        self.assertEqual(view["stored"], "NOT_SYNCHRONIZED_2")
        self.assertEqual(view["public"], "NOT_SYNCHRONIZED")
        self.assertIsNotNone(view["sync"])
        self.assertIn(view["sync"]["stage"], [s.name for s in model.Stage])
        await wait_until(lambda: h.instances[0].state.per_pile == [S, S], timeout=6, message="synchronized")
        self.assertIsNone(h.instances[0].state_json()["piles"][1]["sync"])


class ConcurrencyTest(ReplicationCase):
    async def test_concurrent_admin_commits_on_different_piles(self):
        h = await self.make(3)
        a, b = await asyncio.gather(
            h.instances[0].admin_update([("pile2", PROMOTED)]),
            h.instances[1].admin_update([("pile3", DISC)]),
        )
        self.assertTrue(a.ok, a.error)
        self.assertTrue(b.ok, b.error)
        await wait_until(lambda: h.gens() == [4, 4, 4], message="two admin commits plus promote")
        for inst in h.instances:
            self.assertEqual(inst.state.per_pile, [S, S, D])
            self.assertEqual(inst.state.primary, 1)
        self.assertCommitsAreConsistent(h)

    async def test_conflicting_commits_end_committed_once_or_with_a_distconf_style_error(self):
        known = {
            "Can't switch to SYNCHRONIZED directly",
            "Request must result in a state with one primary pile",
            "Too many state changes in new configuration",
        }
        for round_no in range(5):
            h = await self.make(3)
            a, b = await asyncio.gather(
                h.instances[0].admin_update([("pile2", DISC)]),
                h.instances[2].admin_update([("pile2", PROMOTED)]),
            )
            self.assertTrue(a.ok or b.ok, (a.error, b.error))
            for r in (a, b):
                if not r.ok:
                    self.assertIn(r.error.message, known, r.error)
            await asyncio.sleep(0.2)
            await wait_until(lambda: len(set(h.gens())) == 1, message="converged (round %d)" % round_no)
            self.assertCommitsAreConsistent(h)
            await h.stop()

    async def test_many_concurrent_updates_all_land(self):
        h = await self.make(3)
        results = await asyncio.gather(*(
            h.instances[k % 2].admin_update([("pile3", DISC)]) for k in range(6)))
        self.assertTrue(all(r.ok for r in results), [r.error for r in results])
        await wait_until(lambda: h.gens()[:2] == [7, 7], message="six no-op commits after the first")
        self.assertEqual(h.gens()[2], 2, "a DISCONNECTED pile is not pushed to again")
        self.assertCommitsAreConsistent(h)


class PrepareSlotTest(ReplicationCase):
    async def test_second_proposal_is_busy_and_abort_frees_the_slot(self):
        h = await self.make(2)
        inst = h.instances[1]
        snap = model.derive(inst.state, updates={0: S}).to_json()
        first = await inst.handle_peer("prepare", 0, {"proposal_id": "a", "snapshot": snap})
        second = await inst.handle_peer("prepare", 0, {"proposal_id": "b", "snapshot": snap})
        self.assertTrue(first["ok"])
        self.assertEqual((second["ok"], second["reason"]), (False, "busy"))
        await inst.handle_peer("abort", 0, {"proposal_id": "a"})
        third = await inst.handle_peer("prepare", 0, {"proposal_id": "b", "snapshot": snap})
        self.assertTrue(third["ok"])

    async def test_wrong_generation_is_refused_with_the_local_generation(self):
        h = await self.make(2)
        snap = model.derive(model.derive(h.instances[1].state, updates={0: S}), updates={0: S}).to_json()
        reply = await h.instances[1].handle_peer("prepare", 0, {"proposal_id": "x", "snapshot": snap})
        self.assertEqual(reply["reason"], "generation")
        self.assertEqual(reply["generation"], 1)

    async def test_unreachable_commit_leaves_the_slot_until_the_ttl(self):
        h = await self.make(2, timings=tiny_timings(prepare_ttl=0.2))
        inst = h.instances[1]
        snap = model.derive(inst.state, updates={0: S}).to_json()
        self.assertTrue((await inst.handle_peer("prepare", 0, {"proposal_id": "a", "snapshot": snap}))["ok"])
        self.assertIsNotNone(inst.replica.prepared)
        await wait_until(lambda: inst.replica.prepared is None, message="prepare ttl")
        self.assertIn("prepare.expired", inst.events.kinds())

    async def test_blocked_sender_is_rejected(self):
        h = await self.make(2)
        h.instances[1].set_blocked([0])
        outcome = await h.instances[0].admin_update([("pile2", PROMOTED)])
        self.assertFalse(outcome.ok)


class LinkTest(ReplicationCase):
    async def test_partition_makes_both_sides_see_each_other_down(self):
        h = await self.make(2)
        h.net.cut_both(0, 1)
        await wait_until(lambda: not h.instances[0].connected(1) and not h.instances[1].connected(0),
                         message="mutual down")
        await wait_until(lambda: "peer.down" in h.instances[0].events.kinds(), message="peer.down event")
        h.net.heal()
        await wait_until(lambda: h.instances[0].connected(1) and h.instances[1].connected(0), message="mutual up")
        await wait_until(lambda: "peer.up" in h.instances[0].events.kinds(), message="peer.up event")

    async def test_block_list_is_symmetric_when_set_on_both_sides(self):
        h = await self.make(2)
        h.instances[0].set_blocked([1])
        h.instances[1].set_blocked([0])
        self.assertFalse(h.instances[0].connected(1))
        self.assertFalse(h.instances[1].connected(0))
        h.instances[0].set_blocked([])
        h.instances[1].set_blocked([])
        await wait_until(lambda: h.instances[0].connected(1), message="reconnect")

    async def test_one_way_block_is_asymmetric(self):
        h = await self.make(2)
        h.instances[0].set_blocked([1])
        await wait_until(lambda: not h.instances[1].connected(0), message="pile2 sees pile1 gone")
        self.assertFalse(h.instances[0].connected(1))

    async def test_killed_pile_is_seen_down_and_up_again(self):
        h = await self.make(2, persist=True)
        await h.kill(1)
        await wait_until(lambda: not h.instances[0].connected(1), message="down")
        await h.restart(1)
        await wait_until(lambda: h.instances[0].connected(1), message="up")

    async def test_pile_without_node_majority_is_seen_as_not_connected(self):
        h = await self.make(2)
        for f in h.instances[1].node_faults[:2]:
            f.down = True
        h.instances[1].on_nodes_changed()
        await wait_until(lambda: not h.instances[0].connected(1), message="not ready seen by peer")
        self.assertIn("not_ready", h.instances[1].events.kinds())


class ResetTest(ReplicationCase):
    async def test_reset_local_installs_the_initial_snapshot_at_the_given_generation(self):
        h = await self.make(2)
        self.assertTrue((await h.instances[0].admin_update([("pile2", DISC)])).ok)
        await wait_until(lambda: h.gens() == [2, 2])
        for inst in h.instances:
            inst.reset_local(9)
        for inst in h.instances:
            self.assertEqual(inst.state.generation, 9)
            self.assertEqual(inst.state.per_pile, [S, S])
            self.assertEqual(inst.state.sync, {})
        await asyncio.sleep(0.1)
        self.assertEqual(h.gens(), [9, 9])

    async def test_reset_fences_a_proposal_waiting_for_config_quorum(self):
        h = await self.make(3, timings=tiny_timings(quorum_fail_delay=2.0))
        h.net.cut_both(0, 1)
        proposal = asyncio.create_task(h.instances[0].admin_update([("pile2", SUSP)]))
        await wait_until(lambda: "propose.retry" in h.instances[0].events.kinds(),
                         message="proposal retry while config quorum is cut")
        h.instances[0].reset_local(2)
        h.instances[1].reset_local(2)
        h.instances[2].reset_local(2)
        h.net.heal()
        await proposal
        await wait_until(lambda: all(not inst._bg for inst in h.instances), message="background work drained")
        self.assertEqual(h.gens(), [2, 2, 2])
        self.assertFalse(any(e.get("generation", 0) > 2
                             for inst in h.instances for e in inst.events.query(kind="commit")))

    async def test_reset_during_prepare_aborts_the_old_proposal(self):
        h = await self.make(3)
        gate = asyncio.Event()
        prepared = asyncio.Event()
        original_call = h.net.call

        async def gated_prepare(src, dst, op, payload, timeout):
            result = await original_call(src, dst, op, payload, timeout)
            if op == "prepare" and src == 0 and dst == 1:
                prepared.set()
                await gate.wait()
            return result

        h.net.call = gated_prepare
        proposal = asyncio.create_task(h.instances[0].admin_update([("pile2", SUSP)]))
        await asyncio.wait_for(prepared.wait(), timeout=2.0)
        self.assertIsNotNone(h.instances[1].replica.prepared)
        for inst in h.instances:
            inst.reset_local(7)
        gate.set()
        outcome = await asyncio.wait_for(proposal, timeout=2.0)
        await wait_until(lambda: all(not inst._bg for inst in h.instances), message="old proposal drained")
        self.assertFalse(outcome.ok, "a pre-reset proposal must not report success")
        self.assertEqual(h.gens(), [7, 7, 7])
        self.assertTrue(all(inst.replica.prepared is None for inst in h.instances))
        self.assertFalse(any(e.get("generation", 0) > 7
                             for inst in h.instances for e in inst.events.query(kind="commit")))


class FailedPeerCommitTest(ReplicationCase):
    async def test_persistence_failure_keeps_prepared_slot_and_publishes_no_hooks(self):
        h = await self.make(2, persist=True)
        inst = h.instances[1]
        before = inst.state.copy()
        candidate = model.derive(before, updates={1: D})
        inst.replica.prepared = Prepared("proposal", candidate.copy(), 0, 1.0)
        before_tracker = inst.tracker.to_json()
        before_commits = len(inst.events.query(kind="commit"))
        notifications = []
        inst.on_listeners_changed = lambda: notifications.append(True)
        with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                        side_effect=OSError("injected replace failure")):
            with self.assertRaisesRegex(OSError, "injected replace failure"):
                await inst.handle_peer("commit", 0, {
                    "proposal_id": "proposal", "snapshot": candidate.to_json(), "cause": "admin"})
        self.assertEqual(inst.state, before)
        self.assertEqual(inst.replica.prepared.proposal_id, "proposal")
        self.assertEqual(inst.tracker.to_json(), before_tracker)
        self.assertEqual(len(inst.events.query(kind="commit")), before_commits)
        self.assertEqual(notifications, [])

    async def test_failed_reattach_keeps_replica_detached(self):
        h = await self.make(2, persist=True)
        inst = h.instances[1]
        inst.replica.detached = True
        inst.replica.save()
        with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                        side_effect=OSError("injected replace failure")):
            with self.assertRaisesRegex(OSError, "injected replace failure"):
                inst.clear_replica_override()
        self.assertTrue(inst.replica.detached)

    async def test_failed_local_reset_leaves_runtime_state_and_hooks_untouched(self):
        h = await self.make(2, persist=True)
        inst = h.instances[0]
        inst.node_faults[0].unavailable = True
        inst.set_blocked([1])
        inst.tracker.stage[0] = model.Stage.WRITE_KEEP_BARRIER_DONOTKEEP_DATA
        inst.tracker.bsc_cleared[0] = True
        before_state = inst.state.copy()
        before_tracker = inst.tracker.to_json()
        notifications = []
        inst.on_listeners_changed = lambda: notifications.append(True)
        with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                        side_effect=OSError("injected replace failure")):
            with self.assertRaisesRegex(OSError, "injected replace failure"):
                inst.reset_local(9)
        self.assertEqual(inst.state, before_state)
        self.assertTrue(inst.node_faults[0].unavailable)
        self.assertEqual(inst.reach.blocked, {1})
        self.assertEqual(inst.tracker.to_json(), before_tracker)
        self.assertFalse(inst.events.query(kind="reset"))
        self.assertEqual(notifications, [])


class LostReplyNetwork(InMemoryNetwork):
    """The peer handles a `prepare` but its reply never arrives (a stalled link, a late timeout)."""

    def __init__(self) -> None:
        super().__init__()
        self.lose_prepare_reply: set = set()

    async def call(self, src, dst, op, payload, timeout):
        if op == "prepare" and dst in self.lose_prepare_reply:
            target = self.instances[dst]
            await target.handle_peer(op, src, json.loads(json.dumps(payload)))
            await asyncio.sleep(timeout)
            raise PeerError("timeout")
        return await super().call(src, dst, op, payload, timeout)


class ConcurrentProposalsTest(ReplicationCase):
    """Proposals whose required sets do not overlap must still exclude each other (the distconf root does)."""

    async def test_disjoint_required_sets_do_not_split_the_brain(self):
        h = await self.make(2)
        # pile1 wants pile2 out (required: pile1); pile2 wants a failover to itself (required: pile2).
        a, b = await asyncio.gather(
            h.instances[0].admin_update([("pile2", DISC)]),
            h.instances[1].admin_update([("pile1", DISC), ("pile2", PRIMARY)]),
        )
        await asyncio.sleep(0.4)
        self.assertTrue(a.ok or b.ok, (a.error, b.error))
        self.assertCommitsAreConsistent(h)

    async def test_disjoint_required_sets_over_many_seeds(self):
        for round_no in range(6):
            h = await self.make(2)
            h.instances[0].rng.seed(100 + round_no)
            h.instances[1].rng.seed(200 + round_no)
            await asyncio.gather(
                h.instances[0].admin_update([("pile2", DISC)]),
                h.instances[1].admin_update([("pile1", DISC), ("pile2", PRIMARY)]),
            )
            await asyncio.sleep(0.3)
            self.assertCommitsAreConsistent(h)
            await h.stop()

    async def test_three_piles_two_proposers_outside_each_others_required_set(self):
        h = await self.make(3)
        a, b = await asyncio.gather(
            h.instances[0].admin_update([("pile2", DISC)]),
            h.instances[2].admin_update([("pile1", DISC), ("pile3", PRIMARY)]),
        )
        await asyncio.sleep(0.4)
        self.assertTrue(a.ok or b.ok, (a.error, b.error))
        self.assertCommitsAreConsistent(h)

    async def test_an_observer_with_its_own_proposal_in_flight_answers_busy(self):
        h = await self.make(2)
        # pile2 holds a prepared slot for its own proposal; pile1's proposal does not require pile2
        snap = model.derive(h.instances[1].state, updates={0: D}).to_json()
        self.assertTrue((await h.instances[1].handle_peer(
            "prepare", 1, {"proposal_id": "own", "snapshot": snap}))["ok"])
        new = model.derive(h.instances[0].state, updates={1: D})
        result = await h.instances[0]._two_phase(new, "admin")
        self.assertFalse(result.committed, "a pile that may be committing something else must be asked")
        self.assertIsNone(h.instances[0].replica.prepared)
        self.assertEqual(h.instances[0].state.generation, 1)

    async def test_a_dead_or_unready_observer_does_not_block_a_failover(self):
        h = await self.make(2)
        await h.kill(0)
        outcome = await h.instances[1].admin_update([("pile1", DISC), ("pile2", PRIMARY)])
        self.assertTrue(outcome.ok, outcome.error)

    async def test_a_detached_proposer_does_not_report_success(self):
        h = await self.make(2)
        inst = h.instances[0]
        inst.override_replica({}, detach=True)
        # a failover to the other pile: the proposer is outside the required set
        new = model.derive(inst.state, updates={0: D}, primary=1, promoted=1)
        result = await inst._two_phase(new, "admin")
        self.assertFalse(result.committed, "the proposer could not install what it reports as committed")
        self.assertEqual(inst.state.generation, 1)


class CancelledProposalTest(ReplicationCase):
    async def test_cancelling_the_caller_does_not_wedge_the_pile(self):
        h = await self.make(2, timings=tiny_timings(prepare_ttl=5.0, quorum_fail_delay=0.5))
        h.net.delays[(0, 1)] = 0.1
        # PROMOTED needs both piles to accept, so a prepare is in flight when the client gives up
        task = asyncio.get_running_loop().create_task(h.instances[0].admin_update([("pile2", PROMOTED)]))
        await asyncio.sleep(0.03)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        h.net.delays.clear()
        # the commit goes on without the client, as the distconf actor does; nothing stays prepared
        await wait_until(lambda: h.gens() == [3, 3], message="commit and promote finished")
        self.assertTrue(all(i.replica.prepared is None for i in h.instances))
        outcome = await h.instances[0].admin_update([("pile1", PROMOTED)])
        self.assertTrue(outcome.ok, outcome.error)
        self.assertCommitsAreConsistent(h)

    async def test_cancelling_the_pipeline_aborts_every_prepared_slot(self):
        h = await self.make(2, timings=tiny_timings(prepare_ttl=5.0))
        h.net.delays[(0, 1)] = 0.15
        new = model.derive(h.instances[0].state, updates={1: Stored.SUSPENDED})   # both piles must accept
        task = asyncio.get_running_loop().create_task(h.instances[0]._two_phase(new, "admin"))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        h.net.delays.clear()
        await wait_until(lambda: all(i.replica.prepared is None for i in h.instances),
                         timeout=1.0, message="aborts reached everybody before the ttl")
        self.assertEqual(h.gens(), [1, 1])

    async def test_a_stopped_instance_abandons_its_proposals_and_leaves_no_slots(self):
        h = await self.make(2, timings=tiny_timings(prepare_ttl=5.0))
        h.net.delays[(0, 1)] = 0.15
        task = asyncio.get_running_loop().create_task(h.instances[0].admin_update([("pile2", PROMOTED)]))
        await asyncio.sleep(0.05)
        await h.instances[0].stop()
        h.net.delays.clear()
        await asyncio.gather(task, return_exceptions=True)
        await wait_until(lambda: h.instances[1].replica.prepared is None, timeout=1.0,
                         message="the peer's slot released by an abort, not by the ttl")
        self.assertEqual(h.gens(), [1, 1], "a stopped instance must not finish a commit")

    async def test_a_timed_out_prepare_is_aborted_on_the_peer(self):
        net = LostReplyNetwork()
        h = Harness(2, timings=tiny_timings(prepare_ttl=5.0))
        h.net = net
        self.harnesses.append(h)
        await h.start()
        net.lose_prepare_reply.add(1)
        new = model.derive(h.instances[0].state, updates={1: Stored.SUSPENDED})   # pile2 must accept
        result = await h.instances[0]._two_phase(new, "admin")
        self.assertFalse(result.committed)
        await wait_until(lambda: h.instances[1].replica.prepared is None, timeout=1.0,
                         message="the slot taken by the lost-reply prepare was aborted")


class CommitWithoutPrepareTest(ReplicationCase):
    async def test_a_late_commit_does_not_evict_another_proposal_at_the_same_generation(self):
        h = await self.make(2)
        inst = h.instances[1]
        mine = model.derive(inst.state, updates={0: D}).to_json()
        late = model.derive(inst.state, updates={0: Stored.SUSPENDED}).to_json()
        self.assertTrue((await inst.handle_peer("prepare", 1, {"proposal_id": "q", "snapshot": mine}))["ok"])
        reply = await inst.handle_peer("commit", 0, {"proposal_id": "p", "snapshot": late, "cause": "admin"})
        self.assertFalse(reply["installed"])
        self.assertEqual(inst.state.generation, 1)
        self.assertEqual(inst.replica.prepared.proposal_id, "q", "q still owns the slot")
        reply = await inst.handle_peer("commit", 1, {"proposal_id": "q", "snapshot": mine, "cause": "admin"})
        self.assertTrue(reply["installed"])
        self.assertEqual(inst.state.per_pile[0], D)

    async def test_a_commit_after_the_slot_expired_is_still_installed_when_nothing_conflicts(self):
        h = await self.make(2)
        inst = h.instances[1]
        snap = model.derive(inst.state, updates={0: Stored.SUSPENDED}).to_json()
        reply = await inst.handle_peer("commit", 0, {"proposal_id": "p", "snapshot": snap, "cause": "admin"})
        self.assertTrue(reply["installed"])


class ReplicaFileTest(ReplicationCase):
    def make_instance(self, state_dir):
        topo = Topology(["pile1", "pile2"], 3, timings=tiny_timings())
        return PileInstance(topo, 0, InMemoryNetwork(), state_dir=state_dir, rng=random.Random(1))

    async def test_a_corrupt_replica_file_stops_the_start_instead_of_resetting(self):
        d = os.path.join(self._tmp.name, "pile1")
        os.makedirs(d)
        path = os.path.join(d, "replica.json")
        with open(path, "w") as f:
            f.write("{not json")
        with self.assertRaises(ReplicaError):
            await self.make_instance(d).start()
        with open(path) as f:
            self.assertEqual(f.read(), "{not json", "the file is left alone for the operator")

    async def test_a_replica_file_of_another_cluster_size_stops_the_start(self):
        d = os.path.join(self._tmp.name, "pile1")
        os.makedirs(d)
        path = os.path.join(d, "replica.json")
        with open(path, "w") as f:
            json.dump({"state": model.initial_state(3).to_json(), "detached": False}, f)
        with self.assertRaises(ReplicaError):
            await self.make_instance(d).start()

    async def test_a_missing_file_bootstraps(self):
        d = os.path.join(self._tmp.name, "pile1")
        inst = self.make_instance(d)
        await inst.start()
        self.assertEqual(inst.state.generation, 1)
        self.assertTrue(os.path.exists(os.path.join(d, "replica.json")))
        await inst.stop()


class PeerSnapshotValidationTest(ReplicationCase):
    def bad_snapshots(self, inst):
        good = model.derive(inst.state, updates={1: D}).to_json()
        yield "three piles in a two pile cluster", dict(good, per_pile=good["per_pile"] + [3])
        yield "primary out of range", dict(good, primary=7)
        yield "promoted out of range", dict(good, promoted=-1)
        yield "unknown stored state", dict(good, per_pile=[3, 9])

    async def test_push_commit_and_prepare_reject_malformed_snapshots(self):
        h = await self.make(2)
        inst = h.instances[1]
        for label, snap in self.bad_snapshots(inst):
            for op in ("push", "commit", "prepare"):
                with self.subTest(label, op=op):
                    with self.assertRaises((ValueError, KeyError)):
                        await inst.handle_peer(op, 0, {"proposal_id": "x", "snapshot": snap, "cause": "admin"})
                    self.assertEqual(inst.state.generation, 1)
                    self.assertEqual(inst.state.num_piles(), 2)
                    self.assertIsNone(inst.replica.prepared)


class RuntimeTimingsTest(ReplicationCase):
    async def test_heartbeat_timeout_changes_apply_to_a_running_instance(self):
        h = await self.make(2, timings=tiny_timings(heartbeat_timeout=30.0))
        await h.kill(1)
        await asyncio.sleep(0.15)
        self.assertTrue(h.instances[0].connected(1), "the long timeout still holds the dead pile as connected")
        h.instances[0].timings.update({"heartbeat_timeout": 0.1})
        await wait_until(lambda: not h.instances[0].connected(1), timeout=1.0,
                         message="the new timeout applied")


class SuspendedToDisconnectedSyncTest(ReplicationCase):
    async def test_the_follow_up_keeps_the_sync_record_of_the_suspension(self):
        h = await self.make(3)
        self.assertTrue((await h.instances[0].admin_update([("pile3", SUSP)])).ok)
        await wait_until(lambda: h.gens() == [3, 3, 3], message="suspend -> disconnected")
        self.assertEqual(h.instances[0].state.sync, {2: 2},
                         "NotifyBridgeSuspended leaves the pile sync state as the suspension set it")


if __name__ == "__main__":
    unittest.main()
