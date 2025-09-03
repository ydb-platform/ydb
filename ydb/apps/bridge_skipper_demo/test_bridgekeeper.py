#! /usr/bin/python3
#
# For now, we test only the case when there are 2 piles: pile1, pile2
# Test cases, initial state is pile1 is primary, pile2 is synchronized.
# 1. Both piles return config with the same generation.
# Bridge's _decide() must return empty cmd list. Piles in state from get_state_and_history() should be is_good().
# 2. Both report pile1 as bad. _decide should return failover for primary
# current_state.get_endpoints(pile2) must return endpoint for pile2. Piles in state from get_state_and_history():
# pile1 not is_good(), pile is good.
# 3. Both report pile2 as bad. _decide should keep current primary and failover pile2 (endpoint is for pile1).
# In state pile1 is_good() and pile2 not is_good().
# 4. pile1 reports pile2 as bad, pile2 reports pile1 as bad. Both responsive. Keep current
# primary and failover pile2. pile1 is_good(), pile2 not.
# 5. pile1 reports pile2 as bad, pile2 is unresponsive. Keep current
# primary and failover pile2. current_state.get_endpoints(primary) must return
# endpoint from pile1. pile1 is good, pile2 not.
# 6. pile1 is unresponsive, pile2 reports pile1 as bad and reports itself as good.
# failover primary to pile2. current_state.get_endpoints(pile2) must return endpoint for pile2.
# pile1 not is_good(), pile2 is good.
# 7. pile1 is unresponsive, pile2 reports both piles as bad. _decide() must return nothing.
# both piles must be not is_good().
# 8. both piles are unresponsive.  _decide() must return nothing.
# both piles must be not is_good().
#
# 9. pile1 returns generation 1 where pile1 is primary, pile2 returns generation 2 where it is primary.
# Check that BridgeSkipper accepts gen2 and pile2 as primary.

import logging
import unittest

from unittest.mock import patch

import time

import bridge
import health


# Add custom TRACE log level (lower than DEBUG)
TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.TRACE = TRACE_LEVEL_NUM
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kws)

    logging.Logger.trace = trace


def _mk_endpoint_result(self_check_result: str = "GOOD", bad_piles=None):
    if bad_piles is None:
        bad_piles = []
    return health.EndpointHealthCheckResult(self_check_result, list(bad_piles))


def _mk_pile_health(name: str, self_check_result: str = "GOOD", bad_piles=None, endpoints=None, responsive=True, stale=False):
    if not responsive:
        if stale:
            # Return an object that is older than TTL so is_responsive() is False
            if bad_piles is None:
                bad_piles = []
            if endpoints is None:
                endpoints = [f"{name}-e1", f"{name}-e2"]
            real_self_check = "EMERGENCY" if len(bad_piles) > 0 else (self_check_result or "GOOD")
            results = {ep: _mk_endpoint_result(real_self_check, bad_piles) for ep in endpoints[:2]}
            ph = health.PileHealth(name, results)
            ph.monoTs = time.monotonic() - (health.HEALTH_REPLY_TTL_SECONDS + 1.0)
            return ph
        return None

    if bad_piles is None:
        bad_piles = []
    if endpoints is None:
        endpoints = [f"{name}-e1", f"{name}-e2"]

    # If a pile reports any bad piles (including itself), self_check_result must be EMERGENCY
    real_self_check = "EMERGENCY" if len(bad_piles) > 0 else self_check_result

    results = {ep: _mk_endpoint_result(real_self_check, bad_piles) for ep in endpoints[:2]}
    ph = health.PileHealth(name, results)
    return ph


def _mk_admin_states(gen: int, primary: str):
    other = "pile2" if primary == "pile1" else "pile1"
    piles = {
        "pile1": health.PileAdminItem("pile1", "PRIMARY" if primary == "pile1" else "SYNCHRONIZED"),
        "pile2": health.PileAdminItem("pile2", "PRIMARY" if primary == "pile2" else "SYNCHRONIZED"),
    }
    return health.PileAdminStates(gen, piles)


class TestBridgeSkipperDecisions(unittest.TestCase):
    def setUp(self):
        self.initial_piles = {
            "pile1": ["p1a", "p1b", "p1c"],
            "pile2": ["p2a", "p2b", "p2c"],
        }

    def _make_keeper(self, get_health_state_return):
        with patch("bridge.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
            runner_instance = FakeRunner.return_value
            runner_instance.start.return_value = None
            runner_instance.get_health_state.return_value = get_health_state_return

            keeper = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True)
            # Ensure our mocked get_health_state is used for this instance
            keeper.async_checker = runner_instance
            return keeper

    def test_case_1_same_generation_all_good(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", []),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", []),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(keeper.current_state)
        self.assertTrue(keeper.current_state.piles["pile1"].is_good())
        self.assertTrue(keeper.current_state.piles["pile2"].is_good())
        self.assertIsNotNone(decision)
        primary, commands = decision
        self.assertEqual(primary, "pile1")
        self.assertEqual(commands, [])

    def test_case_2_both_report_pile1_bad(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile1"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile1"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile2")
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0],
            ["--assume-yes", "admin", "cluster", "bridge", "failover", "--pile", "pile1", "--new-primary", "pile2"],
        )

        state, _ = keeper.get_state_and_history()
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertTrue(state.piles["pile2"].is_good())
        self.assertTrue(all(ep.startswith("pile2-") for ep in state.get_endpoints("pile2")))

    def test_case_3_both_report_pile2_bad(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile2"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile1")
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0],
            ["--assume-yes", "admin", "cluster", "bridge", "failover", "--pile", "pile2"],
        )

        state, _ = keeper.get_state_and_history()
        self.assertTrue(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_4_mutual_blame_both_responsive_keep_primary(self):
        # pile1 says pile2 bad; pile2 says pile1 bad; both responsive
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile1"]),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile1")
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0],
            ["--assume-yes", "admin", "cluster", "bridge", "failover", "--pile", "pile2"],
        )

        state, _ = keeper.get_state_and_history()
        self.assertTrue(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_5_pile2_unresponsive_keep_primary_failover_pile2(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile1")
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0],
            ["--assume-yes", "admin", "cluster", "bridge", "failover", "--pile", "pile2"],
        )

        state, _ = keeper.get_state_and_history()
        self.assertTrue(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())
        eps = state.get_endpoints("pile1")
        self.assertTrue(len(eps) > 0)
        self.assertTrue(all(ep.startswith("pile1-") for ep in eps))

    def test_case_5b_pile2_stale_unresponsive_keep_primary_failover_pile2(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", [], responsive=False, stale=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile1")
        self.assertEqual(len(commands), 1)
        self.assertIn("pile2", commands[0])

        state, _ = keeper.get_state_and_history()
        self.assertTrue(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_6_pile1_unresponsive_pile2_good_failover_to_pile2(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile1"], responsive=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile2")
        self.assertEqual(len(commands), 1)
        self.assertEqual(
            commands[0],
            ["--assume-yes", "admin", "cluster", "bridge", "failover", "--pile", "pile1", "--new-primary", "pile2"],
        )

        state, _ = keeper.get_state_and_history()
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertTrue(state.piles["pile2"].is_good())
        eps = state.get_endpoints("pile2")
        self.assertTrue(len(eps) > 0)
        self.assertTrue(all(ep.startswith("pile2-") for ep in eps))

    def test_case_7_pile1_unresponsive_pile2_thinks_all_bad_no_decision(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile1", "pile2"], responsive=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNone(decision)
        state, _ = keeper.get_state_and_history()
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_8_both_unresponsive_no_decision(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNone(decision)
        state, _ = keeper.get_state_and_history()
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_8b_both_stale_unresponsive_no_decision(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", [], responsive=False, stale=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", [], responsive=False, stale=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNone(decision)
        state, _ = keeper.get_state_and_history()
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile2"].is_good())

    def test_case_9_generation_switch_accept_newer_primary(self):
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", []),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", []),
                admin_states=_mk_admin_states(2, primary="pile2"),
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile2")
        self.assertEqual(commands, [])


if __name__ == "__main__":
    unittest.main()