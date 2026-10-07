#!/usr/bin/env python3
"""Tests for the cold-start / incremental-resume logic in ``flaky_tests_history.py``.

Run from ``.github/scripts/tests/mute``: ``python3 -m unittest discover -s tests``
(running from the repo root shadows the installed ``ydb`` package).
"""
import datetime
import sys
import unittest
from pathlib import Path
from unittest import mock

_HERE = Path(__file__).resolve().parent
_MUTE_DIR = _HERE.parent
_TESTS_DIR = _MUTE_DIR.parent
_SCRIPTS_DIR = _TESTS_DIR.parent
for _p in (str(_TESTS_DIR), str(_SCRIPTS_DIR), str(_SCRIPTS_DIR / 'analytics')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import flaky_tests_history as fth  # noqa: E402


class DetermineStartDateTest(unittest.TestCase):
    def setUp(self):
        self.ydb_wrapper = mock.Mock()
        self.end_date = datetime.date(2026, 7, 14)

    def _call(self, start_date_override=None, end_date_override=None):
        return fth.determine_start_date(
            self.ydb_wrapper,
            test_runs_table='test_runs',
            flaky_tests_table='flaky_tests_window',
            build_type='relwithdebinfo',
            branch='stable-x',
            start_date_override=start_date_override,
            end_date_override=end_date_override,
        )

    def test_explicit_override_wins(self):
        override = datetime.date(2026, 6, 1)
        start, end = self._call(start_date_override=override, end_date_override=self.end_date)
        self.assertEqual(start, override)
        self.assertEqual(end, self.end_date)

    def test_recent_history_resumes_without_gap(self):
        # Normal steady-state case: last recorded day is yesterday -> resume exactly
        # there (no artificial cold-start-style jump, no gap).
        recent = self.end_date - datetime.timedelta(days=1)
        with mock.patch.object(fth, 'get_max_date_from_history', return_value=recent):
            start, end = self._call(end_date_override=self.end_date)
        self.assertEqual(start, recent)
        self.assertEqual(end, self.end_date)

    def test_moderately_stale_history_still_resumes_from_last_day(self):
        # A gap smaller than MAX_RESUME_GAP_DAYS must not be capped — this is exactly
        # the "avoid history gaps" behavior the PR intentionally introduced.
        stale = self.end_date - datetime.timedelta(days=fth.MAX_RESUME_GAP_DAYS - 10)
        with mock.patch.object(fth, 'get_max_date_from_history', return_value=stale):
            start, end = self._call(end_date_override=self.end_date)
        self.assertEqual(start, stale)

    def test_very_stale_history_is_capped_not_unbounded(self):
        # Collection paused for ~2 years: resuming from the true last-recorded day
        # would re-create the unbounded-backfill problem this rework was meant to
        # avoid. Must be capped to MAX_RESUME_GAP_DAYS instead.
        ancient = self.end_date - datetime.timedelta(days=730)
        with mock.patch.object(fth, 'get_max_date_from_history', return_value=ancient):
            start, end = self._call(end_date_override=self.end_date)
        expected = self.end_date - datetime.timedelta(days=fth.MAX_RESUME_GAP_DAYS)
        self.assertEqual(start, expected)
        self.assertGreater(start, ancient)

    def test_cold_start_uses_min_run_date_capped_at_default_days_back(self):
        very_old_run = self.end_date - datetime.timedelta(days=400)
        with mock.patch.object(fth, 'get_max_date_from_history', return_value=None), \
             mock.patch.object(fth, 'get_min_date_from_test_runs', return_value=very_old_run):
            start, end = self._call(end_date_override=self.end_date)
        self.assertEqual(start, self.end_date - datetime.timedelta(days=fth.DEFAULT_DAYS_BACK))

    def test_cold_start_no_data_anywhere_uses_default_days_back(self):
        with mock.patch.object(fth, 'get_max_date_from_history', return_value=None), \
             mock.patch.object(fth, 'get_min_date_from_test_runs', return_value=None):
            start, end = self._call(end_date_override=self.end_date)
        self.assertEqual(start, self.end_date - datetime.timedelta(days=fth.DEFAULT_DAYS_BACK))


if __name__ == '__main__':
    unittest.main()
