import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib.connectivity import Reachability


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class ReachabilityTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.own_ready = True
        self.r = Reachability(0, 3, timeout=1.0, clock=self.clock, own_ready=lambda: self.own_ready)

    def test_self_follows_own_readiness(self):
        self.assertTrue(self.r.connected(0))
        self.own_ready = False
        self.assertFalse(self.r.connected(0))

    def test_before_the_first_ping_a_peer_counts_as_connected(self):
        self.assertTrue(self.r.connected(1))
        self.clock.advance(0.5)
        self.assertTrue(self.r.connected(1))

    def test_a_peer_that_never_answers_expires_after_the_grace_period(self):
        self.clock.advance(1.01)
        self.assertFalse(self.r.connected(1))

    def test_ping_keeps_a_peer_connected_until_the_timeout(self):
        self.r.record_ok(1, True, 5)
        self.clock.advance(0.9)
        self.assertTrue(self.r.connected(1))
        self.clock.advance(0.2)
        self.assertFalse(self.r.connected(1))
        self.assertFalse(self.r.reachable(1))

    def test_ready_false_means_not_connected_but_reachable(self):
        self.r.record_ok(1, False, 5)
        self.assertTrue(self.r.reachable(1))
        self.assertFalse(self.r.connected(1))
        self.r.record_ok(1, True, 5)
        self.assertTrue(self.r.connected(1))

    def test_block_list_cuts_immediately(self):
        self.r.record_ok(1, True, 5)
        self.r.blocked.add(1)
        self.assertFalse(self.r.connected(1))
        self.assertFalse(self.r.reachable(1))
        self.assertTrue(self.r.connected(2))
        self.r.blocked.clear()
        self.assertTrue(self.r.connected(1))

    def test_generation_is_remembered(self):
        self.r.record_ok(2, True, 9)
        self.assertEqual(self.r.last_generation[2], 9)

    def test_evaluate_reports_transitions_once(self):
        self.r.record_ok(1, True, 1)
        self.r.record_ok(2, True, 1)
        self.assertEqual(self.r.evaluate(), [])
        self.clock.advance(1.5)
        self.r.record_ok(2, True, 1)
        self.assertEqual(self.r.evaluate(), [(1, False)])
        self.assertEqual(self.r.evaluate(), [])
        self.r.record_ok(1, True, 1)
        self.assertEqual(self.r.evaluate(), [(1, True)])

    def test_restart_forgets_everything(self):
        self.r.record_ok(1, True, 7)
        self.clock.advance(5)
        self.r.restart()
        self.assertTrue(self.r.connected(1))
        self.assertEqual(self.r.last_generation, {})

    def test_to_json(self):
        self.r.record_ok(1, True, 4)
        self.r.blocked.add(2)
        out = {x["pile"]: x for x in self.r.to_json(["a", "b", "c"])}
        self.assertEqual(sorted(out), ["b", "c"])
        self.assertTrue(out["b"]["connected"])
        self.assertEqual(out["b"]["generation"], 4)
        self.assertTrue(out["c"]["blocked"])
        self.assertFalse(out["c"]["connected"])


if __name__ == "__main__":
    unittest.main()
