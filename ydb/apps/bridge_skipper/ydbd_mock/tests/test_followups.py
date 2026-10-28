import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import followups, model
from ydb.apps.bridge_skipper.ydbd_mock.lib.followups import Due
from ydb.apps.bridge_skipper.ydbd_mock.lib.model import ClusterState, Stage, Stored, SyncTracker
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Timings

D, N1, N2, S, X = (Stored.DISCONNECTED, Stored.NOT_SYNCHRONIZED_1, Stored.NOT_SYNCHRONIZED_2,
                   Stored.SYNCHRONIZED, Stored.SUSPENDED)


def st(per_pile, primary=0, promoted=None, generation=1, **kw):
    return ClusterState(per_pile=list(per_pile), primary=primary,
                        promoted=primary if promoted is None else promoted, generation=generation, **kw)


def due(state, tracker=None, connected=lambda p: True):
    return followups.due_followups(state, tracker or SyncTracker(), connected)


class DueTest(unittest.TestCase):
    def test_nothing_due_in_a_healthy_cluster(self):
        self.assertEqual(due(st([S, S, S])), [])

    def test_promote(self):
        self.assertEqual(due(st([S, S], primary=0, promoted=1)), [Due("promote", 1)])

    def test_promote_waits_while_the_promoted_pile_is_not_synchronized(self):
        self.assertEqual(due(st([S, D], primary=0, promoted=1)), [])

    def test_suspend_to_disconnected(self):
        self.assertEqual(due(st([S, X])), [Due("suspend", 1)])

    def test_connect_needs_connectivity(self):
        s = st([S, N1], sync={1: 2})
        self.assertEqual(due(s), [Due("connect", 1)])
        self.assertEqual(due(s, connected=lambda p: p != 1), [])

    def test_sync_step_for_n2(self):
        s = st([S, N2], sync={1: 2})
        self.assertEqual(due(s), [Due("sync_step", 1)])

    def test_sync_pauses_while_not_connected(self):
        s = st([S, N2], sync={1: 2})
        self.assertEqual(due(s, connected=lambda p: p != 1), [])

    def test_bsc_done_after_the_last_stage(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.stage[1] = Stage.SYNCED
        self.assertEqual(due(s, t), [Due("bsc_done", 1)])

    def test_synchronized_when_everything_is_done(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.stage[1] = Stage.SYNCED
        t.bsc_cleared[1] = True
        self.assertEqual(due(s, t), [Due("synchronized", 1)])

    def test_synchronized_does_not_need_connectivity(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.stage[1] = Stage.SYNCED
        t.bsc_cleared[1] = True
        self.assertEqual(due(s, t, connected=lambda p: False), [Due("synchronized", 1)])

    def test_disconnected_pile_has_nothing_due(self):
        self.assertEqual(due(st([S, D], sync={1: 2})), [])

    def test_several_at_once(self):
        s = st([S, N1, X, S], primary=0, promoted=3, sync={1: 2, 2: 2})
        got = {d.key for d in due(s)}
        self.assertEqual(got, {("promote", 3), ("connect", 1), ("suspend", 2)})

    def test_commit_flag(self):
        self.assertTrue(Due("promote", 0).commits)
        self.assertTrue(Due("synchronized", 0).commits)
        self.assertFalse(Due("sync_step", 0).commits)
        self.assertFalse(Due("bsc_done", 0).commits)


class BuildCommitTest(unittest.TestCase):
    def test_promote(self):
        s = st([S, S], primary=0, promoted=1, generation=4)
        new = followups.build_commit(s, Due("promote", 1))
        self.assertEqual((new.primary, new.promoted, new.generation), (1, 1, 5))
        self.assertEqual(new.per_pile, [S, S])

    def test_suspend_to_disconnected_keeps_the_sync_record_of_the_suspension(self):
        s = st([S, X], generation=4, sync={1: 3})
        new = followups.build_commit(s, Due("suspend", 1))
        self.assertEqual(new.per_pile, [S, D])
        self.assertEqual(new.sync, {1: 3})

    def test_connect(self):
        s = st([S, N1], generation=6, sync={1: 3})
        new = followups.build_commit(s, Due("connect", 1))
        self.assertEqual(new.per_pile, [S, N2])
        self.assertEqual(new.generation, 7)
        self.assertEqual(new.sync, {1: 3})

    def test_synchronized_drops_the_sync_record(self):
        s = st([S, N2], generation=8, sync={1: 3})
        new = followups.build_commit(s, Due("synchronized", 1))
        self.assertEqual(new.per_pile, [S, S])
        self.assertEqual(new.sync, {})

    def test_stale_conditions_give_none(self):
        self.assertIsNone(followups.build_commit(st([S, S]), Due("promote", 1)))
        self.assertIsNone(followups.build_commit(st([S, D]), Due("suspend", 1)))
        self.assertIsNone(followups.build_commit(st([S, N2]), Due("connect", 1)))
        self.assertIsNone(followups.build_commit(st([S, N1]), Due("synchronized", 1)))
        self.assertIsNone(followups.build_commit(st([S, S], primary=0, promoted=1, ), Due("promote", 0)))

    def test_local_only_kinds_never_commit(self):
        self.assertIsNone(followups.build_commit(st([S, N2]), Due("sync_step", 1)))
        self.assertIsNone(followups.build_commit(st([S, N2]), Due("bsc_done", 1)))

    def test_whole_resync_chain_is_one_commit_per_step(self):
        # D -> (rejoin) N1 -> connect N2 -> synchronized: every automatic step is its own generation.
        s = st([S, N1], generation=10, sync={1: 3})
        s = followups.build_commit(s, Due("connect", 1))
        self.assertEqual(s.generation, 11)
        s = followups.build_commit(s, Due("synchronized", 1))
        self.assertEqual(s.generation, 12)
        self.assertEqual(s.per_pile, [S, S])


class DelayTest(unittest.TestCase):
    def test_delays_and_stagger(self):
        t = Timings(promote_delay=1.0, suspend_delay=2.0, connect_delay=3.0, sync_step_delay=4.0,
                    followup_stagger=0.1)
        self.assertEqual(followups.delay_for(Due("promote", 0), 0, t), 1.0)
        self.assertAlmostEqual(followups.delay_for(Due("promote", 0), 2, t), 1.2)
        self.assertEqual(followups.delay_for(Due("suspend", 0), 0, t), 2.0)
        self.assertEqual(followups.delay_for(Due("connect", 0), 0, t), 3.0)
        self.assertEqual(followups.delay_for(Due("sync_step", 0), 0, t), 4.0)
        self.assertEqual(followups.delay_for(Due("bsc_done", 0), 0, t), 4.0)
        self.assertEqual(followups.delay_for(Due("synchronized", 0), 0, t), 0.0)


class SyncProgressTest(unittest.TestCase):
    """The tracker and the due rules together: stage progression, pause, and the _1 no-reset hazard."""

    def run_steps(self, state, tracker, connected=lambda p: True, limit=20):
        steps = 0
        while steps < limit:
            kinds = [d for d in due(state, tracker, connected) if d.kind in ("sync_step", "bsc_done")]
            if not kinds:
                return steps
            tracker.step(kinds[0].pile)
            steps += 1
        return steps

    def test_progress_runs_to_synchronized(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.reset_from(s)
        self.assertEqual(self.run_steps(s, t), 5)
        self.assertEqual(due(s, t), [Due("synchronized", 1)])

    def test_pause_while_disconnected_does_not_reset(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.reset_from(s)
        t.step(1)
        t.step(1)
        self.assertEqual(self.run_steps(s, t, connected=lambda p: False), 0)
        self.assertEqual(t.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP)
        self.assertEqual(self.run_steps(s, t), 3)
        self.assertEqual(due(s, t), [Due("synchronized", 1)])

    def test_back_to_n1_keeps_the_stage_hazard(self):
        s = st([S, N2], sync={1: 2})
        t = SyncTracker()
        t.reset_from(s)
        t.step(1)
        t.step(1)
        t.step(1)
        n1 = model.derive(s, updates={1: N1})
        t.on_commit(s, n1)
        self.assertEqual(t.stage[1], Stage.WRITE_KEEP_BARRIER_DONOTKEEP_DATA)
        n2 = followups.build_commit(n1, Due("connect", 1))
        t.on_commit(n1, n2)
        # only the last stage and the BSC flag remain: the data hazard the review flagged
        self.assertEqual(self.run_steps(n2, t), 2)

    def test_disconnect_resets_progress(self):
        s = st([S, N2], sync={1: 2}, generation=2)
        t = SyncTracker()
        t.reset_from(s)
        for _ in range(4):
            t.step(1)
        d = model.derive(s, updates={1: D})
        t.on_commit(s, d)
        self.assertEqual(t.stage[1], Stage.BLOCKS)


if __name__ == "__main__":
    unittest.main()
