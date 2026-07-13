#!/usr/bin/env python3

import unittest
from datetime import datetime, timezone

from export_issues_to_ydb import (
    build_open_periods,
    extract_last_close_actor,
    open_period_rows_for_issue,
)


def _dt(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


class BuildOpenPeriodsTest(unittest.TestCase):
    def test_never_closed(self):
        issue = {
            'state': 'OPEN',
            'timelineItems': {'nodes': []},
        }
        periods = build_open_periods(issue, _dt('2026-01-01T10:00:00+00:00'), None)
        self.assertEqual(periods, [{'start': '2026-01-01', 'end': None}])

    def test_closed_once(self):
        issue = {
            'state': 'CLOSED',
            'timelineItems': {
                'nodes': [
                    {
                        '__typename': 'ClosedEvent',
                        'createdAt': '2026-01-05T14:00:00Z',
                    },
                ],
            },
        }
        periods = build_open_periods(
            issue,
            _dt('2026-01-01T10:00:00+00:00'),
            _dt('2026-01-05T14:00:00+00:00'),
        )
        self.assertEqual(periods, [{'start': '2026-01-01', 'end': '2026-01-05'}])

    def test_close_then_reopen_resets_sla_start(self):
        issue = {
            'state': 'OPEN',
            'timelineItems': {
                'nodes': [
                    {
                        '__typename': 'ClosedEvent',
                        'createdAt': '2026-01-05T14:00:00Z',
                    },
                    {
                        '__typename': 'ReopenedEvent',
                        'createdAt': '2026-01-10T09:00:00Z',
                    },
                ],
            },
        }
        periods = build_open_periods(issue, _dt('2026-01-01T10:00:00+00:00'), None)
        self.assertEqual(
            periods,
            [
                {'start': '2026-01-01', 'end': '2026-01-05'},
                {'start': '2026-01-10', 'end': None},
            ],
        )

    def test_multiple_close_reopen_cycles(self):
        issue = {
            'state': 'CLOSED',
            'timelineItems': {
                'nodes': [
                    {'__typename': 'ClosedEvent', 'createdAt': '2026-01-05T14:00:00Z'},
                    {'__typename': 'ReopenedEvent', 'createdAt': '2026-01-10T09:00:00Z'},
                    {'__typename': 'ClosedEvent', 'createdAt': '2026-01-15T18:00:00Z'},
                ],
            },
        }
        periods = build_open_periods(
            issue,
            _dt('2026-01-01T10:00:00+00:00'),
            _dt('2026-01-15T18:00:00+00:00'),
        )
        self.assertEqual(
            periods,
            [
                {'start': '2026-01-01', 'end': '2026-01-05'},
                {'start': '2026-01-10', 'end': '2026-01-15'},
            ],
        )

    def test_same_day_close_and_reopen(self):
        issue = {
            'state': 'OPEN',
            'timelineItems': {
                'nodes': [
                    {'__typename': 'ClosedEvent', 'createdAt': '2026-01-05T10:00:00Z'},
                    {'__typename': 'ReopenedEvent', 'createdAt': '2026-01-05T15:00:00Z'},
                ],
            },
        }
        periods = build_open_periods(issue, _dt('2026-01-01T10:00:00+00:00'), None)
        self.assertEqual(
            periods,
            [
                {'start': '2026-01-01', 'end': '2026-01-05'},
                {'start': '2026-01-05', 'end': None},
            ],
        )

    def test_unsorted_timeline_events(self):
        issue = {
            'state': 'OPEN',
            'timelineItems': {
                'nodes': [
                    {'__typename': 'ReopenedEvent', 'createdAt': '2026-01-10T09:00:00Z'},
                    {'__typename': 'ClosedEvent', 'createdAt': '2026-01-05T14:00:00Z'},
                ],
            },
        }
        periods = build_open_periods(issue, _dt('2026-01-01T10:00:00+00:00'), None)
        self.assertEqual(
            periods,
            [
                {'start': '2026-01-01', 'end': '2026-01-05'},
                {'start': '2026-01-10', 'end': None},
            ],
        )

    def test_no_created_at_returns_empty(self):
        issue = {'state': 'OPEN', 'timelineItems': {'nodes': []}}
        self.assertEqual(build_open_periods(issue, None, None), [])


class OpenPeriodRowsForIssueTest(unittest.TestCase):
    def test_rows_for_multiple_periods(self):
        exported_at = _dt('2026-07-13T12:00:00+00:00')
        rows = open_period_rows_for_issue(
            46312,
            'repo-46312',
            [
                {'start': '2026-01-01', 'end': '2026-01-05'},
                {'start': '2026-01-10', 'end': None},
            ],
            exported_at,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['issue_number'], 46312)
        self.assertEqual(rows[0]['project_item_id'], 'repo-46312')
        self.assertEqual(rows[0]['period_index'], 0)
        self.assertEqual(rows[0]['period_start'].isoformat(), '2026-01-01')
        self.assertEqual(rows[0]['period_end'].isoformat(), '2026-01-05')
        self.assertEqual(rows[1]['period_index'], 1)
        self.assertEqual(rows[1]['period_start'].isoformat(), '2026-01-10')
        self.assertIsNone(rows[1]['period_end'])
        self.assertEqual(rows[0]['exported_at'], exported_at)


class ExtractLastCloseActorTest(unittest.TestCase):
    def test_ignores_reopened_event(self):
        issue = {
            'timelineItems': {
                'nodes': [
                    {
                        '__typename': 'ReopenedEvent',
                        'createdAt': '2026-01-10T09:00:00Z',
                    },
                    {
                        '__typename': 'ClosedEvent',
                        'createdAt': '2026-01-05T14:00:00Z',
                        'actor': {'__typename': 'User', 'login': 'alice'},
                    },
                ],
            },
        }
        result = extract_last_close_actor(issue)
        self.assertEqual(result['login'], 'alice')
        self.assertEqual(result['event_at'], _dt('2026-01-05T14:00:00+00:00'))


if __name__ == '__main__':
    unittest.main()
