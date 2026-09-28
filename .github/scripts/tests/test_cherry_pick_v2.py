#!/usr/bin/env python3

import datetime
import logging
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cherry_pick_v2 import Source, create_pr_source, sort_sources, to_utc

UTC = datetime.timezone.utc
LOGGER = logging.getLogger("test_cherry_pick_v2")


def make_source(title, is_merged=True, merged_at=None):
    return Source(
        type='pr',
        commit_shas=[title],
        title=title,
        body_item=title,
        author=None,
        pull_requests=[],
        is_merged=is_merged,
        merged_at=merged_at,
    )


def titles(sources):
    return [s.title for s in sources]


class ToUtcTest(unittest.TestCase):
    def test_none(self):
        self.assertIsNone(to_utc(None))

    def test_naive_is_treated_as_utc(self):
        self.assertEqual(to_utc(datetime.datetime(2026, 1, 1, 12)), datetime.datetime(2026, 1, 1, 12, tzinfo=UTC))

    def test_aware_is_converted_to_utc(self):
        tz = datetime.timezone(datetime.timedelta(hours=3))
        self.assertEqual(to_utc(datetime.datetime(2026, 1, 1, 15, tzinfo=tz)), datetime.datetime(2026, 1, 1, 12, tzinfo=UTC))


class SortSourcesTest(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(sort_sources([], LOGGER), [])

    def test_single(self):
        self.assertEqual(titles(sort_sources([make_source('a')], LOGGER)), ['a'])

    def test_merged_sorted_by_merge_time(self):
        sources = [
            make_source('c', merged_at=datetime.datetime(2026, 1, 3, tzinfo=UTC)),
            make_source('a', merged_at=datetime.datetime(2026, 1, 1, tzinfo=UTC)),
            make_source('b', merged_at=datetime.datetime(2026, 1, 2, tzinfo=UTC)),
        ]
        self.assertEqual(titles(sort_sources(sources, LOGGER)), ['a', 'b', 'c'])

    def test_unmerged_go_last_in_input_order(self):
        sources = [
            make_source('u2', is_merged=False),
            make_source('m2', merged_at=datetime.datetime(2026, 1, 2, tzinfo=UTC)),
            make_source('u1', is_merged=False),
            make_source('m1', merged_at=datetime.datetime(2026, 1, 1, tzinfo=UTC)),
        ]
        self.assertEqual(titles(sort_sources(sources, LOGGER)), ['m1', 'm2', 'u2', 'u1'])

    def test_equal_merge_time_keeps_input_order(self):
        t = datetime.datetime(2026, 1, 1, tzinfo=UTC)
        sources = [make_source('b', merged_at=t), make_source('a', merged_at=t)]
        self.assertEqual(titles(sort_sources(sources, LOGGER)), ['b', 'a'])

    def test_unknown_merge_time_goes_first_in_input_order(self):
        sources = [
            make_source('known', merged_at=datetime.datetime(2026, 1, 1, tzinfo=UTC)),
            make_source('unknown2'),
            make_source('unknown1'),
        ]
        self.assertEqual(titles(sort_sources(sources, LOGGER)), ['unknown2', 'unknown1', 'known'])

    def test_naive_and_aware_times_are_comparable(self):
        tz = datetime.timezone(datetime.timedelta(hours=3))
        sources = [
            make_source('b', merged_at=datetime.datetime(2026, 1, 1, 12, 30)),
            make_source('a', merged_at=datetime.datetime(2026, 1, 1, 15, tzinfo=tz)),
        ]
        self.assertEqual(titles(sort_sources(sources, LOGGER)), ['a', 'b'])


def make_commit(sha, parents_count=1):
    return mock.Mock(sha=sha, parents=[mock.Mock()] * parents_count)


def make_pull(merged, merge_commit_sha=None, commits=()):
    pull = mock.Mock(number=1, merged=merged, merge_commit_sha=merge_commit_sha, merged_at=None)
    pull.get_commits.return_value = list(commits)
    return pull


def make_repo(merge_commit_parents_count=1):
    repo = mock.Mock()
    repo.get_commit.return_value = make_commit('merge', merge_commit_parents_count)
    return repo


class CreatePrSourceTest(unittest.TestCase):
    def test_squash_merged_uses_merge_commit(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b')])
        self.assertEqual(create_pr_source(pull, make_repo(), LOGGER).commit_shas, ['merge'])

    def test_merge_commit_uses_individual_commits(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b')])
        self.assertEqual(create_pr_source(pull, make_repo(2), LOGGER).commit_shas, ['a', 'b'])

    def test_unmerged_uses_individual_commits(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('b')])
        self.assertEqual(create_pr_source(pull, make_repo(), LOGGER).commit_shas, ['a', 'b'])

    def test_merge_commits_inside_pr_are_skipped(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('m', 2), make_commit('b')])
        self.assertEqual(create_pr_source(pull, make_repo(), LOGGER).commit_shas, ['a', 'b'])

    def test_no_commits_raises(self):
        pull = make_pull(False, None, [make_commit('m', 2)])
        with self.assertRaises(ValueError):
            create_pr_source(pull, make_repo(), LOGGER)


if __name__ == '__main__':
    unittest.main()
