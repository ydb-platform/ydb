#! /usr/bin/python3

import health as health_mod

import logging
import unittest

from unittest.mock import patch
from concurrent.futures import Future


# Add custom TRACE log level (lower than DEBUG)
TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.TRACE = TRACE_LEVEL_NUM
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

    def trace(self, message, *args, **kws):
        if self.isEnabledFor(TRACE_LEVEL_NUM):
            self._log(TRACE_LEVEL_NUM, message, args, **kws)

    logging.Logger.trace = trace


def make_completed_future(value):
    f = Future()
    f.set_result(value)
    return f


class TestAsyncHealthcheckRunner(unittest.TestCase):
    def setUp(self):
        self.initial_piles = {
            'pile1': ['host1', 'host2', 'host3'],
            'pile2': ['host1', 'host2', 'host3'],
        }
        self.runner = health_mod.AsyncHealthcheckRunner(path_to_cli='cli', initial_piles=self.initial_piles, use_https=False)

    def _make_health_side_effect(self, responses_by_pile):
        def side_effect(pile_name, endpoints, executor, use_https=False):
            results_map = {}
            for ep in endpoints:
                state, bad_list = responses_by_pile[pile_name].get(ep, ('GOOD', []))
                results_map[ep] = health_mod.EndpointHealthCheckResult(state, bad_list)
            return make_completed_future(health_mod.PileHealth(pile_name, results_map))
        return side_effect

    def _make_admin_side_effect(self, generation, states_map):
        def side_effect(path_to_cli, endpoints, executor):
            piles = {name: health_mod.PileAdminItem(name, state) for name, state in states_map.items()}
            return make_completed_future(health_mod.PileAdminStates(generation, piles))
        return side_effect

    def _prime_quorum_health(self):
        # All GOOD everywhere to establish quorum_endpoints for both piles
        responses = {
            'pile1': {ep: ('GOOD', []) for ep in self.initial_piles['pile1']},
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        }
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect(responses)):
            self.runner._do_healthcheck()

    def test_case_1_all_good_and_admin_states(self):
        responses = {
            'pile1': {ep: ('GOOD', []) for ep in self.initial_piles['pile1']},
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        }
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect(responses)):
            self.runner._do_healthcheck()

        admin_states_case = {'pile1': 'PRIMARY', 'pile2': 'SYNCHRONIZED'}
        with patch.object(health_mod, '_async_fetch_pile_list', side_effect=self._make_admin_side_effect(1, admin_states_case)):
            self.runner._update_pile_lists()

        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].health.self_check_result, 'GOOD')
        self.assertEqual(state['pile1'].health.bad_piles, [])
        self.assertEqual(state['pile2'].health.self_check_result, 'GOOD')
        self.assertEqual(state['pile2'].health.bad_piles, [])
        self.assertEqual(set(state['pile1'].health.quorum_endpoints), set(self.initial_piles['pile1']))
        self.assertEqual(set(state['pile2'].health.quorum_endpoints), set(self.initial_piles['pile2']))
        self.assertIsNotNone(state['pile1'].admin_states)
        self.assertEqual(state['pile1'].admin_states.piles['pile1'].admin_reported_state, 'PRIMARY')
        self.assertEqual(state['pile2'].admin_states.piles['pile2'].admin_reported_state, 'SYNCHRONIZED')

    def test_case_2_primary_one_emergency_still_good(self):
        # Establish quorum for admin fetch step if used
        self._prime_quorum_health()

        responses = {
            'pile1': {
                'host1': ('GOOD', []),
                'host2': ('EMERGENCY', ['pile2']),
                'host3': ('GOOD', []),
            },
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        }
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect(responses)):
            self.runner._do_healthcheck()

        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].health.self_check_result, 'GOOD')
        self.assertEqual(state['pile1'].health.bad_piles, [])
        self.assertEqual(set(state['pile1'].health.quorum_endpoints), {'host1', 'host3'})

    def test_case_3_primary_two_emergency_now_bad_pile2(self):
        responses = {
            'pile1': {
                'host1': ('EMERGENCY', ['pile2']),
                'host2': ('EMERGENCY', ['pile2']),
                'host3': ('GOOD', []),
            },
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        }
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect(responses)):
            self.runner._do_healthcheck()

        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].health.self_check_result, 'EMERGENCY')
        self.assertIn('pile2', state['pile1'].health.bad_piles)
        self.assertEqual(set(state['pile1'].health.quorum_endpoints), {'host1', 'host2'})

    def test_case_4_mixed_results_no_majority_selfcheck(self):
        responses = {
            'pile1': {
                'host1': ('GOOD', []),
                'host2': ('EMERGENCY', ['pile2']),
                'host3': ('DEGRADED', ['pile1']),
            },
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        }
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect(responses)):
            self.runner._do_healthcheck()

        state = self.runner.get_health_state()
        self.assertIsNone(state['pile1'].health.self_check_result)
        self.assertEqual(state['pile1'].health.bad_piles, [])
        self.assertEqual(state['pile1'].health.quorum_endpoints, [])

    def test_admin_states_primary_then_swap(self):
        # Ensure quorum endpoints exist
        self._prime_quorum_health()

        # First mapping: pile1 PRIMARY, pile2 SYNCHRONIZED
        with patch.object(health_mod, '_async_fetch_pile_list', side_effect=self._make_admin_side_effect(1, {'pile1': 'PRIMARY', 'pile2': 'SYNCHRONIZED'})):
            self.runner._update_pile_lists()
        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].admin_states.piles['pile1'].admin_reported_state, 'PRIMARY')
        self.assertEqual(state['pile2'].admin_states.piles['pile2'].admin_reported_state, 'SYNCHRONIZED')

        # New runner to test vice versa mapping cleanly
        runner2 = health_mod.AsyncHealthcheckRunner(path_to_cli='cli', initial_piles=self.initial_piles, use_https=False)
        with patch.object(health_mod, '_async_pile_health_check', side_effect=self._make_health_side_effect({
            'pile1': {ep: ('GOOD', []) for ep in self.initial_piles['pile1']},
            'pile2': {ep: ('GOOD', []) for ep in self.initial_piles['pile2']},
        })):
            runner2._do_healthcheck()
        with patch.object(health_mod, '_async_fetch_pile_list', side_effect=self._make_admin_side_effect(1, {'pile1': 'SYNCHRONIZED', 'pile2': 'PRIMARY'})):
            runner2._update_pile_lists()
        state2 = runner2.get_health_state()
        self.assertEqual(state2['pile1'].admin_states.piles['pile1'].admin_reported_state, 'SYNCHRONIZED')
        self.assertEqual(state2['pile2'].admin_states.piles['pile2'].admin_reported_state, 'PRIMARY')

    def test_admin_states_generation_does_not_decrease(self):
        # Ensure quorum endpoints exist
        self._prime_quorum_health()

        # First, generation 2
        with patch.object(health_mod, '_async_fetch_pile_list', side_effect=self._make_admin_side_effect(2, {'pile1': 'PRIMARY', 'pile2': 'SYNCHRONIZED'})):
            self.runner._update_pile_lists()
        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].admin_states.generation, 2)
        self.assertEqual(state['pile2'].admin_states.generation, 2)

        # Then, generation 1 (should be ignored)
        with patch.object(health_mod, '_async_fetch_pile_list', side_effect=self._make_admin_side_effect(1, {'pile1': 'PRIMARY', 'pile2': 'SYNCHRONIZED'})):
            self.runner._update_pile_lists()
        state = self.runner.get_health_state()
        self.assertEqual(state['pile1'].admin_states.generation, 2)
        self.assertEqual(state['pile2'].admin_states.generation, 2)


if __name__ == '__main__':
    unittest.main()
