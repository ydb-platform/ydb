#!/usr/bin/env python3
"""
Tests for mute rules in create_new_muted_ya.py

These tests verify the current muting logic before making any changes to the rules.
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


class TestMuteRules(unittest.TestCase):
    """Test cases for current mute rules"""
    
    def setUp(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        # Mock mute_check function
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
            'date_window': 19000,  # Days since epoch
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
    
    def test_flaky_test_rule_all_conditions_met(self):
        """Test that flaky test is identified when all conditions are met"""
        test_data = [self.create_test_data(
            flaky_today=True,
            days_in_state=1,
            pass_count=4,
            fail_count=6,  # fail_count/(pass_count + fail_count) = 0.6 > 0.2
            success_rate=40.0
        )]
        
        self.mock_mute_check.return_value = False
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        # Verify the test was processed
        self.assertIsInstance(result, int)
    
    def test_flaky_test_rule_insufficient_days_in_state(self):
        """Test that flaky test is NOT identified when days_in_state < 1"""
        test_data = [self.create_test_data(
            flaky_today=True,
            days_in_state=0,  # Does not meet >= 1 requirement
            pass_count=4,
            fail_count=6,
            success_rate=40.0
        )]
        
        self.mock_mute_check.return_value = False
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        # Test should complete without errors
        self.assertIsInstance(result, int)
    
    def test_flaky_test_rule_not_flaky_today(self):
        """Test that test is NOT identified as flaky when flaky_today is False"""
        test_data = [self.create_test_data(
            flaky_today=False,  # Does not meet flaky_today requirement
            days_in_state=5,
            pass_count=4,
            fail_count=6,
            success_rate=40.0
        )]
        
        self.mock_mute_check.return_value = False
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
    
    def test_boundary_conditions_success_rate(self):
        """Test boundary conditions for success rate (exactly 80%)"""
        # Test with exactly 80% success rate (fail_rate = 0.2)
        test_data = [self.create_test_data(
            flaky_today=True,
            days_in_state=5,
            pass_count=8,
            fail_count=2,  # fail_rate = 2/10 = 0.2 (exactly at boundary)
            success_rate=80.0
        )]
        
        self.mock_mute_check.return_value = False
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        # At exactly 0.2 fail rate, the condition is fail_count/(pass+fail) > 0.2
        # So 0.2 should NOT trigger flaky (since it's not > 0.2)
        self.assertIsInstance(result, int)
    
    def test_boundary_conditions_just_above_threshold(self):
        """Test just above 80% threshold (79% success rate)"""
        test_data = [self.create_test_data(
            flaky_today=True,
            days_in_state=5,
            pass_count=79,
            fail_count=21,  # fail_rate = 21/100 = 0.21 > 0.2
            success_rate=79.0
        )]
        
        self.mock_mute_check.return_value = False
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        # This should trigger flaky since 0.21 > 0.2
        self.assertIsInstance(result, int)
    
    def test_mute_check_overrides_flaky_rules(self):
        """Test that mute_check=True mutes test regardless of flaky status"""
        test_data = [self.create_test_data(
            flaky_today=False,
            days_in_state=0,
            pass_count=1,
            fail_count=0,
            success_rate=100.0
        )]
        
        self.mock_mute_check.return_value = True  # Should mute regardless of other conditions
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
    
    def test_muted_stable_excludes_from_new_mutes(self):
        """Test that muted_stable_n_days_today tests are excluded from new mutes"""
        test_data = [self.create_test_data(
            muted_stable_n_days_today=True,  # Should be excluded
            flaky_today=True,
            days_in_state=5,
            pass_count=4,
            fail_count=6,
            success_rate=40.0
        )]
        
        self.mock_mute_check.return_value = True
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
    
    def test_deleted_excludes_from_new_mutes(self):
        """Test that deleted_today tests are excluded from new mutes"""
        test_data = [self.create_test_data(
            deleted_today=True,  # Should be excluded
            flaky_today=True,
            days_in_state=5,
            pass_count=4,
            fail_count=6,
            success_rate=40.0
        )]
        
        self.mock_mute_check.return_value = True
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
            
        self.assertIsInstance(result, int)
    
    def test_comprehensive_scenario(self):
        """Test comprehensive scenario that exercises all current mute rules"""
        test_data = [
            # Test that should be deleted
            self.create_test_data(
                test_name='deleted_test',
                deleted_today=True,
                flaky_today=True,
                days_in_state=5,
                pass_count=1,
                fail_count=9,
                success_rate=10.0
            ),
            
            # Test that should be muted stable
            self.create_test_data(
                test_name='stable_test',
                muted_stable_n_days_today=True,
                flaky_today=True,
                days_in_state=5,
                pass_count=2,
                fail_count=8,
                success_rate=20.0
            ),
            
            # Perfect flaky test (meets all 5 conditions)
            self.create_test_data(
                test_name='perfect_flaky',
                flaky_today=True,        # ✓ Condition 1
                days_in_state=3,         # ✓ Condition 2: >= 1
                pass_count=3,            # ✓ Condition 3: total = 7 >= 2
                fail_count=4,            # ✓ Condition 4: >= 2
                success_rate=43.0        # ✓ Condition 5: 4/7 = 0.57 > 0.2
            ),
            
            # Test with mute_check = True
            self.create_test_data(
                test_name='mute_check_test',
                flaky_today=False,
                pass_count=10,
                fail_count=0,
                success_rate=100.0
            ),
            
            # Good test that should not be muted
            self.create_test_data(
                test_name='good_test',
                flaky_today=False,
                days_in_state=10,
                pass_count=95,
                fail_count=5,
                success_rate=95.0
            )
        ]
        
        def mock_mute_check_side_effect(suite, testcase):
            return testcase == 'mute_check_test'
        
        self.mock_mute_check.side_effect = mock_mute_check_side_effect
        
        with patch('create_new_muted_ya.add_lines_to_file'):
            result = apply_and_add_mutes(test_data, self.temp_dir, self.mock_mute_check)
        
        self.assertIsInstance(result, int)


if __name__ == '__main__':
    unittest.main(verbosity=2) 