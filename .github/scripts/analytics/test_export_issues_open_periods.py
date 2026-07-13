#!/usr/bin/env python3

import unittest
from datetime import datetime, timezone

from export_issues_to_ydb import build_open_periods


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
                        'createdAt': '2026-01-05T12:00:00Z',
                    },
                ],
            },
        }
        periods = build_open_periods(
            issue,
            _dt('2026-01-01T10:00:00+00:00'),
            _dt('2026-01-05T12:00:00+00:00'),
        )
        self.assertEqual(periods, [{'start': '2026-01-01', 'end': '2026-01-05'}])

    def test_close_and_reopen(self):
        issue = {
            'state': 'OPEN',
            'timelineItems': {
                'nodes': [
                    {
                        '__typename': 'ClosedEvent',
                        'createdAt': '2026-01-05T12:00:00Z',
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

    def test_multiple_cycles(self):
        issue = {
            'state': 'CLOSED',
            'timelineItems': {
                'nodes': [
                    {'__typename': 'ClosedEvent', 'createdAt': '2026-01-05T12:00:00Z'},
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


if __name__ == '__main__':
    unittest.main()
