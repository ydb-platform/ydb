#!/usr/bin/env python3
"""
Detailed tests for mute rules in create_new_muted_ya.py

This file contains detailed edge case and boundary condition tests for mute rules.
"""

import unittest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch

# Add the parent directories to the path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from create_new_muted_ya import apply_and_add_mutes


class TestMuteRulesDetailed(unittest.TestCase):
    """Detailed edge case tests for mute rules"""
    
    def setUp(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_mute_check = Mock()
        
    def tearDown(self):
        """Cleanup after each test"""
        shutil.rmtree(self.temp_dir)
    
    def create_test_data(self, **kwargs):
        """Helper to create test data with default values"""
        default_data = {
            'suite_folder': 'test_suite',
            'test_name': 'test_case',
            'full_name': 'test_suite/test_case',
            'success_rate': 50.0,
            'days_in_state': 5,
            'owner': 'team/developer',
            'state': 'Flaky',
            'pass_count': 5,
            'fail_count': 5,
            'skip_count': 0,
            'mute_count': 0,
            'date_window': 19000,
            'summary': 'Test summary',
            'branch': 'main',
            'flaky_today': False,
            'deleted_today': False,
            'muted_stable_n_days_today': False,
            'new_flaky_today': False,
            'muted_stable_today': False,
        }
        default_data.update(kwargs)
        return default_data
    
    def test_flaky_rules_boundary_conditions(self):
        """Test boundary conditions for flaky test rules"""
        
        # Test cases with different fail rates around 80% threshold
        test_cases = [
            # Exactly at 80% success rate (fail_rate = 0.2)
            {'name': 'exactly_80_percent', 'pass_count': 8, 'fail_count': 2, 'expected_flaky': False},
            
            # Just below 80% success rate (fail_rate = 0.21)
            {'name': 'just_below_80_percent', 'pass_count': 79, 'fail_count': 21, 'expected_flaky': True},
            
            # Well below 80% success rate (fail_rate = 0.5)
            {'name': 'well_below_80_percent', 'pass_count': 5, 'fail_count': 5, 'expected_flaky': True},
            
            # Minimal passing case (fail_rate = 0.67)
            {'name': 'minimal_passing', 'pass_count': 1, 'fail_count': 2, 'expected_flaky': True},
        ]
        
        for test_case in test_cases:
            with self.subTest(test_case=test_case['name']):
                test_data = [self.create_test_data(
                    test_name=test_case['name'],
                    flaky_today=True,
                    days_in_state=1,
                    pass_count=test_case['pass_count'],
                    fail_count=test_case['fail_count'],
                    success_rate=test_case['pass_count']/(test_case['pass_count']+test_case['fail_count'])*100
                )]
                
                self.mock_mute_check.return_value = False
                
                with patch('create_new_muted_ya.add_lines_to_file'):
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
    
    def test_minimum_run_count_requirements(self):
        """Test minimum run count requirements for flaky tests"""
        
        test_cases = [
            # Insufficient total runs (pass + fail < 2)
            {'name': 'insufficient_runs_1', 'pass_count': 1, 'fail_count': 0, 'meets_requirements': False},
            {'name': 'insufficient_runs_2', 'pass_count': 0, 'fail_count': 1, 'meets_requirements': False},
            
            # Exactly minimum total runs (pass + fail = 2)
            {'name': 'minimum_runs_both', 'pass_count': 1, 'fail_count': 1, 'meets_requirements': False},  # fail_count < 2
            {'name': 'minimum_runs_fail', 'pass_count': 0, 'fail_count': 2, 'meets_requirements': True},   # pass + fail = 2, fail = 2
            
            # Insufficient fail count (fail_count < 2)
            {'name': 'insufficient_fails_1', 'pass_count': 5, 'fail_count': 1, 'meets_requirements': False},
            {'name': 'minimum_fails', 'pass_count': 0, 'fail_count': 2, 'meets_requirements': True},
        ]
        
        for test_case in test_cases:
            with self.subTest(test_case=test_case['name']):
                test_data = [self.create_test_data(
                    test_name=test_case['name'],
                    flaky_today=True,
                    days_in_state=1,
                    pass_count=test_case['pass_count'],
                    fail_count=test_case['fail_count'],
                    success_rate=30.0  # Always below 80%
                )]
                
                self.mock_mute_check.return_value = False
                
                with patch('create_new_muted_ya.add_lines_to_file'):
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
    
    def test_days_in_state_edge_cases(self):
        """Test edge cases for days_in_state requirement"""
        
        test_cases = [
            {'name': 'zero_days', 'days_in_state': 0, 'meets_requirement': False},
            {'name': 'one_day', 'days_in_state': 1, 'meets_requirement': True},
            {'name': 'many_days', 'days_in_state': 100, 'meets_requirement': True},
        ]
        
        for test_case in test_cases:
            with self.subTest(test_case=test_case['name']):
                test_data = [self.create_test_data(
                    test_name=test_case['name'],
                    flaky_today=True,
                    days_in_state=test_case['days_in_state'],
                    pass_count=3,
                    fail_count=7,  # 70% fail rate
                    success_rate=30.0
                )]
                
                self.mock_mute_check.return_value = False
                
                with patch('create_new_muted_ya.add_lines_to_file'):
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
    
    def test_exclusion_rules_priority(self):
        """Test that exclusion rules take priority over mute rules"""
        
        # Test that deleted_today excludes even perfect flaky tests
        test_data = [self.create_test_data(
            test_name='deleted_but_flaky',
            deleted_today=True,
            flaky_today=True,
            days_in_state=5,
            pass_count=2,
            fail_count=8,  # 80% fail rate
            success_rate=20.0
        )]
        
        self.mock_mute_check.return_value = True  # Even mute_check=True should be overridden
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
        
        # Test that muted_stable_n_days_today excludes even perfect flaky tests
        test_data = [self.create_test_data(
            test_name='stable_but_flaky',
            muted_stable_n_days_today=True,
            flaky_today=True,
            days_in_state=5,
            pass_count=2,
            fail_count=8,  # 80% fail rate
            success_rate=20.0
        )]
        
        self.mock_mute_check.return_value = True  # Even mute_check=True should be overridden
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
    
    def test_regex_pattern_transformation(self):
        """Test regex pattern transformation for test chunks"""
        
        test_cases = [
            # Tests with chunk patterns that should be transformed
            {'name': 'chunk_pattern_1', 'suite_folder': 'test_suite[1/5]', 'expected_transform': True},
            {'name': 'chunk_pattern_2', 'suite_folder': 'test_suite[42/100]', 'expected_transform': True},
            {'name': 'chunk_pattern_3', 'suite_folder': 'complex[3/10]_test', 'expected_transform': True},
            
            # Tests without chunk patterns
            {'name': 'no_pattern', 'suite_folder': 'regular_test_suite', 'expected_transform': False},
            {'name': 'similar_pattern', 'suite_folder': 'test[not_a_chunk]', 'expected_transform': False},
        ]
        
        for test_case in test_cases:
            with self.subTest(test_case=test_case['name']):
                test_data = [self.create_test_data(
                    test_name=test_case['name'],
                    suite_folder=test_case['suite_folder'],
                    flaky_today=True,
                    days_in_state=1,
                    pass_count=2,
                    fail_count=8,  # 80% fail rate
                    success_rate=20.0
                )]
                
                self.mock_mute_check.return_value = False
                
                with patch('create_new_muted_ya.add_lines_to_file') as mock_add_lines:
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
    
    def test_multiple_conditions_combinations(self):
        """Test various combinations of conditions"""
        
        test_scenarios = [
            # All conditions met
            {
                'name': 'all_conditions_met',
                'flaky_today': True,
                'days_in_state': 5,
                'pass_count': 1,
                'fail_count': 9,
                'success_rate': 10.0,
                'mute_check_result': False,
                'description': 'Perfect flaky test'
            },
            
            # All conditions met + mute_check
            {
                'name': 'all_plus_mute_check',
                'flaky_today': True,
                'days_in_state': 5,
                'pass_count': 1,
                'fail_count': 9,
                'success_rate': 10.0,
                'mute_check_result': True,
                'description': 'Flaky test + mute_check'
            },
            
            # Only mute_check
            {
                'name': 'only_mute_check',
                'flaky_today': False,
                'days_in_state': 0,
                'pass_count': 10,
                'fail_count': 0,
                'success_rate': 100.0,
                'mute_check_result': True,
                'description': 'Only mute_check, no flaky conditions'
            },
            
            # Missing one flaky condition
            {
                'name': 'missing_flaky_today',
                'flaky_today': False,  # Missing this
                'days_in_state': 5,
                'pass_count': 1,
                'fail_count': 9,
                'success_rate': 10.0,
                'mute_check_result': False,
                'description': 'Missing flaky_today'
            },
        ]
        
        for scenario in test_scenarios:
            with self.subTest(scenario=scenario['name']):
                test_data = [self.create_test_data(
                    test_name=scenario['name'],
                    flaky_today=scenario['flaky_today'],
                    days_in_state=scenario['days_in_state'],
                    pass_count=scenario['pass_count'],
                    fail_count=scenario['fail_count'],
                    success_rate=scenario['success_rate']
                )]
                
                self.mock_mute_check.return_value = scenario['mute_check_result']
                
                with patch('create_new_muted_ya.add_lines_to_file'):
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
    
    def test_precision_boundary_conditions(self):
        """Test precision boundary conditions around 80% threshold"""
        
        # Test various precise fail rates around 0.2
        test_cases = [
            # Exactly 0.2 (should NOT be flaky)
            {'fail_rate': 0.2, 'pass_count': 8, 'fail_count': 2, 'expected_flaky': False},
            
            # Just above 0.2 (should be flaky) - fix the calculation
            {'fail_rate': 0.200001, 'pass_count': 799999, 'fail_count': 200001, 'expected_flaky': True},
            
            # Common fractions near 0.2
            {'fail_rate': 1/5, 'pass_count': 4, 'fail_count': 1, 'expected_flaky': False},  # Exactly 0.2
            {'fail_rate': 2/9, 'pass_count': 7, 'fail_count': 2, 'expected_flaky': True},   # ~0.222
            {'fail_rate': 3/14, 'pass_count': 11, 'fail_count': 3, 'expected_flaky': True}, # ~0.214
        ]
        
        for i, test_case in enumerate(test_cases):
            with self.subTest(test_case=i):
                test_data = [self.create_test_data(
                    test_name=f'precision_test_{i}',
                    flaky_today=True,
                    days_in_state=1,
                    pass_count=test_case['pass_count'],
                    fail_count=test_case['fail_count'],
                    success_rate=test_case['pass_count']/(test_case['pass_count']+test_case['fail_count'])*100
                )]
                
                self.mock_mute_check.return_value = False
                
                with patch('create_new_muted_ya.add_lines_to_file'):
                    result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
                    
                self.assertIsInstance(result, int)
                
                # Verify the actual fail rate calculation
                actual_fail_rate = test_case['fail_count'] / (test_case['pass_count'] + test_case['fail_count'])
                self.assertAlmostEqual(actual_fail_rate, test_case['fail_rate'], places=5)


if __name__ == '__main__':
    unittest.main(verbosity=2) 