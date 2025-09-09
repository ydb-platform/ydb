#! /usr/bin/python3
#
# For now, we test only the case when there are 2 piles: pile1, pile2
#
# Basic test cases, initial state is pile1 is primary, pile2 is synchronized.
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
# 9a. Both piles report good. pile1 is primary. Advance (mock time mono and wall) time
# to HEALTH_REPLY_TTL_SECONDS / 2 and pile2 must report it is good (without reporting pile1 as bad).
# Advance HEALTH_REPLY_TTL_SECONDS / 2 + 1. Check that decide returns empty cmd list.
# 9b. Similarly, but this time pile1 becomes unresponsive. Again, skipper should not decide anything.
#
# 10. pile1 returns generation 1 where pile1 is primary, pile2 returns generation 2 where it is primary.
# Check that BridgeSkipper accepts gen2 and pile2 as primary.
# 10b. pile1 returns generation 1 where pile1 is primary, pile2 returns generation 2 where it is primary.
# pile2 becomes unresponsive. pile1 still returns generation1. Keeper should still use previously
# seen generation1.
#
# 11. network split between piles and split between keepr and pile1, then keeper and pile2 (with pile1 restored)
# - pile1 and pile2 are good, pile1 is primary. Both have generation=1.
# - pile1 becomes unresponsive, pile2 reports pile1 as bad. Failover.
# - next healthcheck obtained from pile2 shows that pile2 is PRIMARY and pile1 DISCONNECTED,
# generation must be 2.
# - then, pile2 becomes unresponsive. pile1 is back with generation1 reporting it as primary
# and pil2 as secondary. Also, it reports pile2 as bad. decide_() should do no actions.
#
# 12. The same as 11. Except that pile2 becomes unresponsive before we get new admin state
# with gen2 + additional scenarious
#
# 13. Both piles have generation 2. pile1 is PRIMARY, pile2 admin state is DISCONNECTED. pile2 reports pile1
# as bad and itself as good. Nothing should happen, pile1 should be good (not marked bad)
#
# Test cases, which emulate skipper (keeper) restart:
#
# 14. Should start with empty non-existing state.
# 15. Should fail to start when path to state has non-existing item.
# 16. Start with exmpty state, run healthcheck, check that state is saved.
# 17. Start with existing state, check that it is loaded.
#
# 18. As 10b. But also we emulate skipper restart. It should read proper generation from saved state and ignore
# older generations

import bridge
import skipper
import health

import logging
import unittest

from unittest.mock import patch

import time
import os
import tempfile


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
        with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
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

        # apply decision (wrap execute_cli_command)
        with patch("bridge.execute_cli_command", return_value=object()) as exec_mock:
            keeper._apply_decision(new_primary, commands)
            self.assertTrue(exec_mock.called)
            # Validate CLI args used for application
            args, kwargs = exec_mock.call_args
            self.assertEqual(args[0], "ydb")  # path_to_cli
            self.assertEqual(args[1], commands[0])  # command
            eps_used = args[2]
            self.assertTrue(len(eps_used) > 0)
            self.assertTrue(all(ep.startswith("pile1-") for ep in eps_used))

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

    def test_case_9a_time_advance_no_decision(self):
        base_mono = {"t": 1000.0}
        base_wall = {"t": 1700000000.0}

        def get_mono():
            return base_mono["t"]

        def get_wall():
            return base_wall["t"]

        with patch("health.time.monotonic", side_effect=get_mono), \
             patch("bridge.time.monotonic", side_effect=get_mono), \
             patch("bridge.time.time", side_effect=get_wall):
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

            # Initial healthcheck at t0
            keeper._do_healthcheck()

            # Advance to TTL/2 and verify pile2 remains good and does not blame pile1
            half = health.HEALTH_REPLY_TTL_SECONDS / 2.0
            base_mono["t"] += half
            base_wall["t"] += half
            keeper._do_healthcheck()
            state, _ = keeper.get_state_and_history()
            self.assertTrue(state.piles["pile2"].is_good())
            self.assertIsNotNone(state.piles["pile2"].pile_health)
            self.assertNotIn("pile1", state.piles["pile2"].pile_health.bad_piles)

            # Advance a bit more (TTL/2 + 1 since start). Still within TTL → no commands
            base_mono["t"] += 1.0
            base_wall["t"] += 1.0
            decision = keeper._decide()
            self.assertIsNotNone(decision)
            new_primary, commands = decision
            self.assertEqual(new_primary, "pile1")
            self.assertEqual(commands, [])

    def test_case_9b_primary_unresponsive_no_decision(self):
        base_mono = {"t": 2000.0}
        base_wall = {"t": 1700005000.0}

        def get_mono():
            return base_mono["t"]

        def get_wall():
            return base_wall["t"]

        with patch("health.time.monotonic", side_effect=get_mono), \
             patch("bridge.time.monotonic", side_effect=get_mono), \
             patch("bridge.time.time", side_effect=get_wall):
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

            # Initial healthcheck at t0
            keeper._do_healthcheck()

            # Advance to TTL/2 and verify state
            half = health.HEALTH_REPLY_TTL_SECONDS / 2.0
            base_mono["t"] += half
            base_wall["t"] += half
            keeper._do_healthcheck()
            state, _ = keeper.get_state_and_history()
            self.assertTrue(state.piles["pile2"].is_good())

            # Now make pile1 unresponsive, keep pile2 responsive
            views["pile1"] = health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            )

            # Advance a bit (TTL/2 + 1 from start) and refresh state
            base_mono["t"] += 1.0
            base_wall["t"] += 1.0
            keeper._do_healthcheck()

            # Even with primary unresponsive, no failover since pile2 doesn't blame pile1
            decision = keeper._decide()
            self.assertIsNotNone(decision)
            new_primary, commands = decision
            self.assertEqual(new_primary, "pile1")
            self.assertEqual(commands, [])

    def test_case_10_generation_switch_accept_newer_primary(self):
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

    def test_case_10b_generation_switch_then_pile2_unresponsive_use_gen1(self):
        # Step 1: pile1 gen=1 primary pile1, pile2 gen=2 primary pile2 → accept gen2
        v1 = {
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

        keeper = self._make_keeper(v1)
        keeper._do_healthcheck()
        d1 = keeper._decide()
        self.assertIsNotNone(d1)
        p1, cmds1 = d1
        self.assertEqual(p1, "pile2")
        self.assertEqual(cmds1, [])

        # Step 2: pile2 becomes unresponsive; pile1 still reports gen=1
        v2 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", []),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=None,
            ),
        }

        keeper.async_checker.get_health_state.return_value = v2
        keeper._do_healthcheck()
        d2 = keeper._decide()
        self.assertIsNotNone(d2)
        p2, cmds2 = d2
        self.assertEqual(p2, "pile2")
        self.assertEqual(cmds2, [])

        state, _ = keeper.get_state_and_history()
        self.assertEqual(state.generation, 2)
        self.assertEqual(state.primary_name, "pile2")

    def test_case_11_network_split_and_generation_shift_then_no_action(self):
        # Step 1: both piles good, gen=1, primary=pile1
        v1 = {
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

        keeper = self._make_keeper(v1)

        keeper._do_healthcheck()
        d1 = keeper._decide()
        self.assertIsNotNone(d1)
        p1, cmds1 = d1
        self.assertEqual(p1, "pile1")
        self.assertEqual(cmds1, [])

        # Step 2: pile1 unresponsive, pile2 reports pile1 bad → failover to pile2
        v2 = {
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
        keeper.async_checker.get_health_state.return_value = v2
        keeper._do_healthcheck()
        d2 = keeper._decide()
        self.assertIsNotNone(d2)
        p2, cmds2 = d2
        self.assertEqual(p2, "pile2")
        self.assertTrue(any("--pile" in c for c in [" ".join(cmd) for cmd in cmds2]))

        # Step 3: new admin list from pile2, gen=2: pile2 PRIMARY, pile1 DISCONNECTED
        piles_gen2 = {
            "pile1": health.PileAdminItem("pile1", "DISCONNECTED"),
            "pile2": health.PileAdminItem("pile2", "PRIMARY"),
        }
        admin_states_gen2 = health.PileAdminStates(2, piles_gen2)
        v3 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", None, [], responsive=False),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", []),
                admin_states=admin_states_gen2,
            ),
        }
        keeper.async_checker.get_health_state.return_value = v3
        keeper._do_healthcheck()
        state_after_v3, _ = keeper.get_state_and_history()
        self.assertEqual(state_after_v3.generation, 2)
        self.assertEqual(state_after_v3.primary_name, "pile2")

        # Step 4: pile2 becomes unresponsive. pile1 is back with gen=1, claims primary and blames pile2
        v4 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=_mk_admin_states(1, primary="pile1"),
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=admin_states_gen2,
            ),
        }
        keeper.async_checker.get_health_state.return_value = v4
        keeper._do_healthcheck()
        d4 = keeper._decide()
        # No good synchronized pile available in latest gen → no decision
        self.assertIsNone(d4)

    def test_case_12_like_11_but_pile2_unresponsive_before_gen2(self):
        # Step 1: both piles good, gen=1, primary=pile1
        v1 = {
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

        keeper = self._make_keeper(v1)
        keeper._do_healthcheck()
        d1 = keeper._decide()
        self.assertIsNotNone(d1)
        p1, cmds1 = d1
        self.assertEqual(p1, "pile1")
        self.assertEqual(cmds1, [])

        # Step 2: pile1 unresponsive, pile2 reports pile1 bad → would failover to pile2
        v2 = {
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
        keeper.async_checker.get_health_state.return_value = v2
        keeper._do_healthcheck()
        d2 = keeper._decide()
        self.assertIsNotNone(d2)
        p2, cmds2 = d2
        self.assertEqual(p2, "pile2")
        self.assertTrue(any("--pile" in c for c in [" ".join(cmd) for cmd in cmds2]))

        # apply decision (wrap execute_cli_command)
        with patch("bridge.execute_cli_command", return_value=object()) as exec_mock:
            keeper._apply_decision(p2, cmds2)
            self.assertTrue(exec_mock.called)
            # Validate CLI args used for application
            args, kwargs = exec_mock.call_args
            self.assertEqual(args[0], "ydb")  # path_to_cli
            self.assertEqual(args[1], cmds2[0])  # command
            eps_used = args[2]
            self.assertTrue(len(eps_used) > 0)
            self.assertTrue(all(ep.startswith("pile2-") for ep in eps_used))

        # Step 3: Before receiving admin gen2, pile2 becomes unresponsive; pile1 returns alive with gen=1 and blames pile2
        v3 = {
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
        keeper.async_checker.get_health_state.return_value = v3
        keeper._do_healthcheck()
        d3 = keeper._decide()
        # There is no latest admin gen2 yet, and now only pile1 is responsive but blames pile2.
        # With no good synchronized pile agreed by everyone, expect no decision.
        self.assertIsNone(d3)

        # Step 4: pile1 still has old generation and reports pile2 as bad – must be ignored
        # Re-check with the same view; decision should remain None
        keeper.async_checker.get_health_state.return_value = v3
        keeper._do_healthcheck()
        d4 = keeper._decide()
        self.assertIsNone(d4)

        # Step 5: pile1 gets proper generation 2 and reports pile2 as bad,
        # pile2 is still unavailable. Must failover primary to pile1
        admin_states_gen2 = health.PileAdminStates(2, {
            "pile1": health.PileAdminItem("pile1", "SYNCHRONIZED"),
            "pile2": health.PileAdminItem("pile2", "PRIMARY"),
        })
        v4 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=admin_states_gen2,
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=None,
            ),
        }
        keeper.async_checker.get_health_state.return_value = v4
        keeper._do_healthcheck()
        d5 = keeper._decide()
        self.assertIsNotNone(d5)
        new_primary_5, cmds5 = d5
        self.assertEqual(new_primary_5, "pile1")
        # Should include failover of old primary pile2 to new primary pile1
        joined5 = [" ".join(c) for c in cmds5]
        self.assertTrue(any("--pile pile2" in c for c in joined5))
        self.assertTrue(any("--new-primary pile1" in c for c in joined5))

        # Step 6: pile1 generation 3: it is primary, pile2 is reported as bad and still unresponsive
        admin_states_gen3 = health.PileAdminStates(3, {
            "pile1": health.PileAdminItem("pile1", "PRIMARY"),
            "pile2": health.PileAdminItem("pile2", "DISCONNECTED"),
        })
        v5 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=admin_states_gen3,
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", None, [], responsive=False),
                admin_states=None,
            ),
        }
        keeper.async_checker.get_health_state.return_value = v5
        keeper._do_healthcheck()
        d6 = keeper._decide()
        self.assertIsNotNone(d6)
        new_primary_6, cmds6 = d6
        self.assertEqual(new_primary_6, "pile1")
        self.assertEqual(cmds6, [])

        # Step 7: pile2 is responsive again, however with generation2. It reports pile1 as bad, must
        # be ignored, since latest generation is 3

        v6 = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "EMERGENCY", ["pile2"], responsive=True),
                admin_states=admin_states_gen3,
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "EMERGENCY", ["pile1"], responsive=True),
                admin_states=admin_states_gen2,  # older gen
            ),
        }
        keeper.async_checker.get_health_state.return_value = v6
        keeper._do_healthcheck()
        d7 = keeper._decide()
        self.assertIsNotNone(d7)
        new_primary_7, cmds7 = d7
        # Must keep gen3 primary pile1 and ignore older gen2 from pile2
        self.assertEqual(new_primary_7, "pile1")
        self.assertEqual(cmds7, [])

    def test_case_13_gen2_primary_pile1_pile2_disconnected_mutual_blame_no_action(self):
        # Admin: gen2 where pile1 PRIMARY and pile2 DISCONNECTED
        admin_states_gen2 = health.PileAdminStates(2, {
            "pile1": health.PileAdminItem("pile1", "PRIMARY"),
            "pile2": health.PileAdminItem("pile2", "DISCONNECTED"),
        })

        # Health: pile2 blames pile1 and thinks itself is good; pile1 reports good
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", [], responsive=True),
                admin_states=admin_states_gen2,
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", ["pile1"], responsive=True),
                admin_states=admin_states_gen2,
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        # Keep pile1 as primary, no commands issued
        self.assertIsNotNone(decision)
        new_primary, commands = decision
        self.assertEqual(new_primary, "pile1")
        self.assertEqual(commands, [])

        state, _ = keeper.get_state_and_history()
        self.assertEqual(state.generation, 2)
        self.assertEqual(state.primary_name, "pile1")
        self.assertTrue(state.piles["pile1"].is_good())
        self.assertFalse(state.piles["pile1"].marked_bad)

    def test_case_13b_gen2_primary_pile1_pile2_disconnected_mutual_blame_no_action(self):
        # Admin: gen2 where pile1 PRIMARY and pile2 DISCONNECTED
        # both report primary1 as bad
        admin_states_gen2 = health.PileAdminStates(2, {
            "pile1": health.PileAdminItem("pile1", "PRIMARY"),
            "pile2": health.PileAdminItem("pile2", "DISCONNECTED"),
        })

        # Health: pile2 blames pile1 and thinks itself is good; pile1 reports good
        views = {
            "pile1": health.PileWorldView(
                "pile1",
                health=_mk_pile_health("pile1", "GOOD", ["pile1"], responsive=True),
                admin_states=admin_states_gen2,
            ),
            "pile2": health.PileWorldView(
                "pile2",
                health=_mk_pile_health("pile2", "GOOD", ["pile1"], responsive=True),
                admin_states=admin_states_gen2,
            ),
        }

        keeper = self._make_keeper(views)
        keeper._do_healthcheck()
        decision = keeper._decide()

        # Keep pile1 as primary, no commands issued
        self.assertIsNone(decision)

        state, _ = keeper.get_state_and_history()
        self.assertEqual(state.generation, 2)
        self.assertEqual(state.primary_name, "pile1")
        self.assertFalse(state.piles["pile1"].is_good())
        self.assertTrue(state.piles["pile1"].marked_bad)

    def test_case_14_start_with_empty_non_existing_state(self):
        state_dir = tempfile.mkdtemp(prefix="keeper_state_dir_")
        state_path = os.path.join(state_dir, "state.bin")
        try:
            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
                runner = FakeRunner.return_value
                runner.start.return_value = None
                # Do not call do_healthcheck to avoid creating the file
                keeper = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper.async_checker = runner
                self.assertIsNone(keeper.current_state)
                self.assertFalse(os.path.exists(state_path))
        finally:
            try:
                os.remove(state_path)
            except Exception:
                pass
            try:
                os.rmdir(state_dir)
            except Exception:
                pass

    def test_case_15_fail_on_non_existing_state_directory(self):
        # Construct a path where parent does not exist
        tmp_base = tempfile.mkdtemp(prefix="keeper_base_")
        nonexist_dir = os.path.join(tmp_base, "no_such_dir")
        state_path = os.path.join(nonexist_dir, "state.bin")
        try:
            # Build argv and run skipper.main expecting SystemExit(2)
            argv = [
                "skipper.py",
                "--endpoint", "dummy-endpoint",
                "--ydb", "/bin/true",
                "--state", state_path,
            ]
            with patch("sys.argv", argv), \
                 patch("bridge.resolve", return_value={"pile1": ["h1","h2","h3"], "pile2": ["h4","h5","h6"]}):
                with self.assertRaises(SystemExit) as ctx:
                    skipper.main()
            self.assertEqual(ctx.exception.code, 2)
        finally:
            try:
                os.rmdir(tmp_base)
            except Exception:
                pass

    def test_case_16_start_empty_then_saved(self):
        fd, state_path = tempfile.mkstemp(prefix="keeper_state_", suffix=".bin")
        os.close(fd)
        try:
            os.remove(state_path)
        except Exception:
            pass
        try:
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
            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
                runner = FakeRunner.return_value
                runner.start.return_value = None
                runner.get_health_state.return_value = views
                keeper = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper.async_checker = runner
                keeper._do_healthcheck()
                self.assertTrue(os.path.exists(state_path))
                self.assertGreater(os.path.getsize(state_path), 0)
        finally:
            try:
                os.remove(state_path)
            except Exception:
                pass

    def test_case_17_start_with_existing_state_then_loaded(self):
        fd, state_path = tempfile.mkstemp(prefix="keeper_state_", suffix=".bin")
        os.close(fd)
        try:
            # First keeper to write state
            views1 = {
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
            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
                runner1 = FakeRunner.return_value
                runner1.start.return_value = None
                runner1.get_health_state.return_value = views1
                keeper1 = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper1.async_checker = runner1
                keeper1._do_healthcheck()
                self.assertTrue(os.path.exists(state_path))

            # Second keeper to load state
            views2 = {
                "pile1": health.PileWorldView(
                    "pile1",
                    health=_mk_pile_health("pile1", "GOOD", []),
                    admin_states=_mk_admin_states(1, primary="pile1"),
                ),
                "pile2": health.PileWorldView(
                    "pile2",
                    health=_mk_pile_health("pile2", None, [], responsive=False),
                    admin_states=None,
                ),
            }
            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner2, patch("bridge.time.sleep", return_value=None):
                runner2 = FakeRunner2.return_value
                runner2.start.return_value = None
                runner2.get_health_state.return_value = views2
                keeper2 = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper2.async_checker = runner2
                # Loaded state should be present before healthcheck
                self.assertIsNotNone(keeper2.current_state)
                self.assertEqual(keeper2.current_state.generation, 2)
                self.assertEqual(keeper2.current_state.primary_name, "pile2")
        finally:
            try:
                os.remove(state_path)
            except Exception:
                pass

    def test_case_18_restart_uses_saved_generation_and_ignores_older(self):
        # Prepare a temp state file
        fd, state_path = tempfile.mkstemp(prefix="keeper_state_", suffix=".bin")
        os.close(fd)
        try:
            # First run: accept gen2, primary pile2
            views1 = {
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

            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner, patch("bridge.time.sleep", return_value=None):
                runner1 = FakeRunner.return_value
                runner1.start.return_value = None
                runner1.get_health_state.return_value = views1

                keeper1 = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper1.async_checker = runner1

                keeper1._do_healthcheck()
                d1 = keeper1._decide()
                self.assertIsNotNone(d1)
                p1, cmds1 = d1
                self.assertEqual(p1, "pile2")
                self.assertEqual(cmds1, [])

            # Second run (restart): older gen1 from pile1, pile2 unresponsive
            views2 = {
                "pile1": health.PileWorldView(
                    "pile1",
                    health=_mk_pile_health("pile1", "GOOD", []),
                    admin_states=_mk_admin_states(1, primary="pile1"),
                ),
                "pile2": health.PileWorldView(
                    "pile2",
                    health=_mk_pile_health("pile2", None, [], responsive=False),
                    admin_states=None,
                ),
            }

            with patch("bridge.health.AsyncHealthcheckRunner") as FakeRunner2, patch("bridge.time.sleep", return_value=None):
                runner2 = FakeRunner2.return_value
                runner2.start.return_value = None
                runner2.get_health_state.return_value = views2

                keeper2 = bridge.BridgeSkipper(path_to_cli="ydb", initial_piles=self.initial_piles, auto_failover=True, state_path=state_path)
                keeper2.async_checker = runner2

                keeper2._do_healthcheck()
                d2 = keeper2._decide()
                self.assertIsNotNone(d2)
                p2, cmds2 = d2
                # Should use saved generation/primary (gen2, pile2) and ignore older gen1
                state2, _ = keeper2.get_state_and_history()
                self.assertEqual(state2.generation, 2)
                self.assertEqual(state2.primary_name, "pile2")
                self.assertEqual(p2, "pile2")
        finally:
            try:
                os.remove(state_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()