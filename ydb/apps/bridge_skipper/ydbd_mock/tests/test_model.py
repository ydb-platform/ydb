import itertools
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, Stage, Stored, SyncTracker

D, N1, N2, S, X = (Stored.DISCONNECTED, Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2,
                   Stored.SYNCHRONIZED, Stored.SUSPENDED)


def st(per_pile, primary=0, promoted=None, generation=1, **kw):
    return ClusterState(per_pile=list(per_pile), primary=primary,
                        promoted=primary if promoted is None else promoted, generation=generation, **kw)


class InitialStateTest(unittest.TestCase):
    def test_initial_state(self):
        s = model.initial_state(3)
        self.assertEqual(s.per_pile, [S, S, S])
        self.assertEqual((s.primary, s.promoted, s.generation), (0, 0, 1))
        self.assertEqual(s.sync, {})
        self.assertEqual(s.quorum_piles_last, [])

    def test_generation_starts_at_one_because_the_demo_treats_zero_as_missing(self):
        self.assertEqual(model.initial_state(2).generation, 1)


class TraitsTest(unittest.TestCase):
    def test_table(self):
        expected = {
            S: (True, True, True),
            N1: (False, False, True),
            N2: (True, True, True),
            D: (False, False, False),
            X: (False, True, True),
            Stored.INACTIVE: (False, False, False),
        }
        self.assertEqual(model.TRAITS, expected)
        for stored, (data, config, connect) in expected.items():
            self.assertEqual(model.requires_data_quorum(stored), data)
            self.assertEqual(model.requires_config_quorum(stored), config)
            self.assertEqual(model.allows_connection(stored), connect)

    def test_stored_numbers_mirror_bridge_proto(self):
        self.assertEqual([int(x) for x in (D, N1, N2, S, X)], [0, 1, 2, 3, 4])

    def test_stage_numbers_mirror_bridge_proto(self):
        self.assertEqual([int(x) for x in Stage], [0, 1, 2, 3, 4])
        self.assertEqual(Stage.SYNCED, 4)


class PublicStateTest(unittest.TestCase):
    def test_every_combination_of_stored_state_and_index(self):
        stored_to_public = {
            D: model.PUB_DISCONNECTED,
            N1: model.PUB_NOT_SYNCHRONIZED,
            N2: model.PUB_NOT_SYNCHRONIZED,
            S: model.PUB_SYNCHRONIZED,
            X: model.PUB_SUSPENDED,
            Stored.INACTIVE: model.PUB_INACTIVE,
        }
        for n in (1, 2, 3):
            for combo in itertools.product(list(Stored), repeat=n):
                for primary in range(n):
                    for promoted in range(n):
                        s = st(combo, primary=primary, promoted=promoted)
                        for i in range(n):
                            if i == primary:
                                want = model.PUB_PRIMARY
                            elif i == promoted:
                                want = model.PUB_PROMOTED
                            else:
                                want = stored_to_public[combo[i]]
                            self.assertEqual(model.public_state(s, i), want, (combo, primary, promoted, i))

    def test_primary_is_checked_before_promoted(self):
        s = st([S, S], primary=0, promoted=0)
        self.assertEqual(model.public_states(s), [model.PUB_PRIMARY, model.PUB_SYNCHRONIZED])

    def test_in_flight_switchover(self):
        s = st([S, S, S], primary=0, promoted=2)
        self.assertEqual(model.public_states(s),
                         [model.PUB_PRIMARY, model.PUB_SYNCHRONIZED, model.PUB_PROMOTED])

    def test_names(self):
        self.assertEqual(model.public_name(model.PUB_PRIMARY), "PRIMARY")
        self.assertEqual(model.public_name(99), "99")
        self.assertEqual(model.public_value("DISCONNECTED"), model.PUB_DISCONNECTED)


class JsonTest(unittest.TestCase):
    def test_round_trip(self):
        s = st([D, N2, X], primary=1, promoted=1, generation=7, quorum_piles_last=["a"], sync={0: 3, 2: 5})
        data = s.to_json()
        self.assertEqual(data["sync"], {"0": 3, "2": 5})
        self.assertEqual(data["per_pile"], [0, 2, 4])
        back = ClusterState.from_json(data)
        self.assertEqual(back, s)
        self.assertIsInstance(back.per_pile[0], Stored)

    def test_missing_optional_fields(self):
        back = ClusterState.from_json({"per_pile": [3, 3], "primary": 0, "promoted": 0, "generation": 1})
        self.assertEqual(back.sync, {})
        self.assertEqual(back.quorum_piles_last, [])

    def test_copy_is_deep(self):
        s = st([S, S], sync={1: 2}, quorum_piles_last=["x"])
        c = s.copy()
        c.per_pile[0] = D
        c.sync[1] = 9
        c.quorum_piles_last.append("y")
        self.assertEqual(s.per_pile[0], S)
        self.assertEqual(s.sync, {1: 2})
        self.assertEqual(s.quorum_piles_last, ["x"])

    def test_describe(self):
        s = st([S, D], generation=4)
        self.assertEqual(s.describe(["pile1", "pile2"]), "gen=4 pile1=PRIMARY pile2=DISCONNECTED")


class DeriveTest(unittest.TestCase):
    def test_generation_plus_one_and_input_untouched(self):
        old = st([S, S], generation=5)
        new = model.derive(old, updates={1: D})
        self.assertEqual(new.generation, 6)
        self.assertEqual(old.generation, 5)
        self.assertEqual(old.per_pile, [S, S])
        self.assertEqual(new.per_pile, [S, D])

    def test_disconnect_marks_unsynced_at_the_new_generation(self):
        new = model.derive(st([S, S], generation=5), updates={1: D})
        self.assertEqual(new.sync, {1: 6})

    def test_suspend_marks_unsynced(self):
        new = model.derive(st([S, S], generation=2), updates={1: X})
        self.assertEqual(new.sync, {1: 3})

    def test_synchronized_drops_the_record(self):
        old = st([S, N2], generation=8, sync={1: 3})
        new = model.derive(old, updates={1: S})
        self.assertEqual(new.sync, {})

    def test_not_synchronized_keeps_the_record(self):
        old = st([S, D], generation=4, sync={1: 2})
        for target in (N1,):
            new = model.derive(old, updates={1: target})
            self.assertEqual(new.sync, {1: 2})
        again = model.derive(st([S, N2], generation=4, sync={1: 2}), updates={1: N1})
        self.assertEqual(again.sync, {1: 2})
        up = model.derive(st([S, N1], generation=4, sync={1: 2}), updates={1: N2})
        self.assertEqual(up.sync, {1: 2})

    def test_no_op_does_not_touch_the_record(self):
        old = st([S, D], generation=4, sync={1: 2})
        new = model.derive(old, updates={1: D})
        self.assertEqual(new.generation, 5)
        self.assertEqual(new.sync, {1: 2})

    def test_primary_promoted_quorum(self):
        new = model.derive(st([S, S, S]), primary=2, promoted=2, quorum_piles=["pile1"])
        self.assertEqual((new.primary, new.promoted, new.quorum_piles_last), (2, 2, ["pile1"]))

    def test_whole_vector(self):
        new = model.derive(st([S, S, S]), per_pile=[S, D, D])
        self.assertEqual(new.per_pile, [S, D, D])
        self.assertEqual(new.sync, {1: 2, 2: 2})


class QuorumSetsTest(unittest.TestCase):
    def test_required_set_is_the_config_quorum_piles_of_the_new_state(self):
        self.assertEqual(model.required_set(st([S, D, N1, N2, X])), [0, 3, 4])

    def test_push_targets_old_or_new_allows_connection(self):
        old = st([S, S, N1])
        new = st([S, D, N1])
        # pile 1 leaves the required set but was connected before; pile 2 is _1 (allows connection)
        self.assertEqual(model.push_targets(old, new), [1, 2])

    def test_a_pile_that_stays_disconnected_is_not_pushed_to(self):
        old = st([S, D])
        new = st([S, D], generation=2)
        self.assertEqual(model.push_targets(old, new), [])

    def test_rejoin_pushes_to_the_returning_pile(self):
        old = st([S, D])
        new = st([S, N1], generation=2)
        self.assertEqual(model.push_targets(old, new), [1])


class SyncTrackerTest(unittest.TestCase):
    def setUp(self):
        self.t = SyncTracker()

    def commit(self, old, new):
        self.t.on_commit(old, new)

    def test_disconnect_creates_a_blocked_entry(self):
        old = st([S, S])
        new = model.derive(old, updates={1: D})
        self.commit(old, new)
        self.assertEqual(self.t.stage[1], Stage.BLOCKS)
        self.assertFalse(self.t.bsc_cleared[1])

    def test_sync_epoch_change_resets_progress_even_when_state_changes_to_n1(self):
        old = st([S, N2], generation=5, sync={1: 2})
        new = st([S, N1], generation=11, sync={1: 10})
        self.t.stage[1] = Stage.SYNCED
        self.t.bsc_cleared[1] = True
        self.t.on_commit(old, new)
        self.assertEqual(self.t.stage[1], Stage.BLOCKS)
        self.assertFalse(self.t.bsc_cleared[1])

    def test_same_sync_epoch_transition_back_to_n1_keeps_progress(self):
        old = st([S, N2], generation=5, sync={1: 2})
        new = st([S, N1], generation=6, sync={1: 2})
        self.t.stage[1] = Stage.WRITE_KEEP_BARRIER_DONOTKEEP_DATA
        self.t.bsc_cleared[1] = False
        self.t.on_commit(old, new)
        self.assertEqual(self.t.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP_DATA)

    def test_stage_advances_to_synced_then_bsc(self):
        old = st([S, S])
        d = model.derive(old, updates={1: D})
        self.commit(old, d)
        n1 = model.derive(d, updates={1: N1})
        self.commit(d, n1)
        n2 = model.derive(n1, updates={1: N2})
        self.commit(n1, n2)
        steps = 0
        while self.t.step(1):
            steps += 1
        self.assertEqual(steps, 5)  # 4 stages, then the BSC flag
        self.assertTrue(self.t.synced(1))
        self.assertTrue(self.t.done(1))
        self.assertFalse(self.t.step(1))

    def test_synced_is_not_done_until_bsc(self):
        for _ in range(4):
            self.t.step(0)
        self.assertTrue(self.t.synced(0))
        self.assertFalse(self.t.done(0))

    def test_not_synchronized_1_does_not_reset_the_stage(self):
        # Hazard copied from real distconf: going back to _1 keeps the stage.
        old = st([S, N2], sync={1: 2})
        self.t.reset_from(old)
        self.t.step(1)
        self.t.step(1)
        self.assertEqual(self.t.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP)
        back = model.derive(old, updates={1: N1})
        self.commit(old, back)
        self.assertEqual(self.t.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP)

    def test_second_disconnect_resets_the_stage(self):
        old = st([S, N2], sync={1: 2}, generation=2)
        self.t.reset_from(old)
        self.t.step(1)
        d = model.derive(old, updates={1: D})
        self.commit(old, d)
        self.assertEqual(self.t.stage[1], Stage.BLOCKS)
        self.assertEqual(d.sync, {1: 3})

    def test_suspend_resets_too(self):
        old = st([S, N2], sync={1: 2})
        self.t.reset_from(old)
        self.t.step(1)
        x = model.derive(old, updates={1: X})
        self.commit(old, x)
        self.assertEqual(self.t.stage[1], Stage.BLOCKS)
        self.assertFalse(self.t.bsc_cleared[1])

    def test_synchronized_drops_the_entry(self):
        old = st([S, N2], sync={1: 2})
        self.t.reset_from(old)
        for _ in range(5):
            self.t.step(1)
        done = model.derive(old, updates={1: S})
        self.commit(old, done)
        self.assertNotIn(1, self.t.stage)
        self.assertNotIn(1, self.t.bsc_cleared)

    def test_snapshot_installed_from_a_peer_seeds_missing_entries(self):
        old = st([S, S])
        new = st([S, N1], generation=9, sync={1: 4})
        self.commit(old, new)
        self.assertEqual(self.t.stage[1], Stage.BLOCKS)

    def test_reset_from_snapshot(self):
        self.t.stage[0] = Stage.SYNCED
        self.t.reset_from(st([S, D], sync={1: 2}))
        self.assertEqual(self.t.stage, {1: Stage.BLOCKS})

    def test_json(self):
        self.t.stage[1] = Stage.WRITE_KEEP
        self.t.bsc_cleared[1] = False
        self.assertEqual(self.t.to_json(), {"stage": {"1": "WRITE_KEEP"}, "bsc_cleared": {"1": False}})


class ReviewFidelityTest(unittest.TestCase):
    def test_suspended_to_disconnected_keeps_the_sync_record_of_the_suspension(self):
        old = st([S, X], generation=7, sync={1: 5})
        new = model.derive(old, updates={1: D})
        self.assertEqual(new.sync, {1: 5}, "NotifyBridgeSuspended does not touch the pile sync state")
        self.assertEqual(new.generation, 8)

    def test_a_fresh_disconnect_still_records_the_new_generation(self):
        new = model.derive(st([S, S]), updates={1: D})
        self.assertEqual(new.sync, {1: 2})

    def test_a_skipped_resync_restarts_the_sync_stage(self):
        # this instance saw N2 with sync record 3; it then missed D, N1 and N2 again (a pull from far ahead)
        old = st([S, N2], generation=4, sync={1: 3})
        tracker = SyncTracker()
        tracker.reset_from(old)
        while not tracker.done(1):
            tracker.step(1)
        new = st([S, N2], generation=9, sync={1: 6})
        tracker.on_commit(old, new)
        self.assertEqual(tracker.stage[1], Stage.BLOCKS)
        self.assertFalse(tracker.bsc_cleared[1])
        self.assertFalse(tracker.done(1))

    def test_an_unchanged_sync_record_keeps_the_stage(self):
        old = st([S, N2], generation=4, sync={1: 3})
        tracker = SyncTracker()
        tracker.reset_from(old)
        tracker.step(1)
        tracker.step(1)
        tracker.on_commit(old, st([S, N2], generation=5, sync={1: 3}))
        self.assertEqual(tracker.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP)


class ValidateSnapshotTest(unittest.TestCase):
    def test_a_good_snapshot_passes(self):
        st([S, D]).validate(2)

    def test_bad_snapshots_raise_value_error(self):
        bad = {
            "wrong number of piles": st([S, S, S]),
            "primary out of range": st([S, S], primary=2),
            "negative primary": st([S, S], primary=-1),
            "promoted out of range": st([S, S], promoted=5),
            "generation below one": st([S, S], generation=0),
            "sync of a pile that does not exist": st([S, S], sync={4: 2}),
        }
        for label, state in bad.items():
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    state.validate(2)


class MockErrorTest(unittest.TestCase):
    def test_str(self):
        self.assertEqual(str(model.MockError(model.BAD_REQUEST, "boom")), "BAD_REQUEST: boom")


if __name__ == "__main__":
    unittest.main()
