#!/usr/bin/env python3

import datetime
import logging
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from github import GithubException

from cherry_pick_v2 import (
    Source,
    build_pr_content,
    collect_sources,
    create_commit_source,
    create_pr_source,
    get_merged_commit_shas,
    pick_linked_pr,
    sort_sources,
    to_utc,
)

UTC = datetime.timezone.utc
LOGGER = logging.getLogger("test_cherry_pick_v2")


class FakePage(list):
    """Stands in for a PyGithub PaginatedList: iterable and get_page()-able"""
    def get_page(self, page):
        return self


def make_source(title, is_merged=True, merged_at=None, incomplete_note=None):
    return Source(
        type='pr',
        commit_shas=[title],
        title=title,
        body_item=title,
        author=None,
        pull_requests=[],
        is_merged=is_merged,
        merged_at=merged_at,
        incomplete_note=incomplete_note,
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


PR_NUMBER = 1
MERGED_AT = datetime.datetime(2026, 1, 1, tzinfo=UTC)
COMMITTED_AT = datetime.datetime(2026, 2, 1, tzinfo=UTC)


def make_commit(sha, parents=('base',), pr_numbers=(), committer_date=None):
    commit = mock.Mock(sha=sha, parents=[mock.Mock(sha=p) for p in parents])
    commit.commit.message = f'Commit {sha}'
    commit.commit.committer.date = committer_date
    commit.get_pulls.return_value = FakePage([mock.Mock(number=n, merged=True) for n in pr_numbers])
    return commit


def make_pull(merged, merge_commit_sha=None, commits=()):
    pull = mock.Mock(
        number=PR_NUMBER,
        merged=merged,
        merge_commit_sha=merge_commit_sha,
        merged_at=MERGED_AT if merged else None,
        commits=len(commits),
    )
    pull.get_commits.return_value = list(commits)
    return pull


def make_repo(*commits):
    by_sha = {c.sha: c for c in commits}
    repo = mock.Mock()
    repo.get_commit.side_effect = lambda sha: by_sha[sha]
    return repo


class CreatePrSourceTest(unittest.TestCase):
    def test_squash_merged_uses_merge_commit(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(make_commit('merge', ['base']), make_commit('base', ['older'], [2]))
        source = create_pr_source(pull, repo, LOGGER)
        self.assertEqual(source.commit_shas, ['merge'])
        self.assertTrue(source.is_merged)
        self.assertEqual(source.merged_at, MERGED_AT)

    def test_rebase_merged_uses_rebased_commits(self):
        pull = make_pull(True, 'r2', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base'], [PR_NUMBER]),
            make_commit('base', ['older'], [2]),
        )
        self.assertEqual(create_pr_source(pull, repo, LOGGER).commit_shas, ['r1', 'r2'])

    def test_rebase_walk_is_limited_by_pr_commit_count(self):
        pull = make_pull(True, 'r2', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['r0'], [PR_NUMBER]),
            make_commit('r0', ['base'], [PR_NUMBER]),
        )
        self.assertEqual(create_pr_source(pull, repo, LOGGER).commit_shas, ['r1', 'r2'])

    def test_partial_rebase_walk_warns_about_missing_commits(self):
        pull = make_pull(True, 'r2', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base']),  # association lost mid-series
        )
        with self.assertLogs(LOGGER, level='WARNING'):
            source = create_pr_source(pull, repo, LOGGER)
        self.assertEqual(source.commit_shas, ['r2'])

    def test_squash_merge_title_suppresses_ambiguity_warning(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b', ['a'])])
        merge_commit = make_commit('merge', ['base'])
        merge_commit.commit.message = f'Fix something (#{PR_NUMBER})'
        repo = make_repo(merge_commit, make_commit('base', ['older'], [2]))
        with self.assertNoLogs(LOGGER, level='WARNING'):
            source = create_pr_source(pull, repo, LOGGER)
        self.assertEqual(source.commit_shas, ['merge'])

    def test_merge_commit_uses_individual_commits(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(make_commit('merge', ['base', 'b']))
        self.assertEqual(create_pr_source(pull, repo, LOGGER).commit_shas, ['a', 'b'])

    def test_force_pushed_branch_warns(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(make_commit('merge', ['base', 'rewritten']))
        with self.assertLogs(LOGGER, level='WARNING'):
            source = create_pr_source(pull, repo, LOGGER)
        self.assertEqual(source.commit_shas, ['a', 'b'])

    def test_unmerged_uses_individual_commits(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('b', ['a'])])
        source = create_pr_source(pull, make_repo(), LOGGER)
        self.assertEqual(source.commit_shas, ['a', 'b'])
        self.assertFalse(source.is_merged)
        self.assertIsNone(source.merged_at)

    def test_merge_commits_inside_pr_are_skipped(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('m', ['a', 'main']), make_commit('b', ['m'])])
        self.assertEqual(create_pr_source(pull, make_repo(), LOGGER).commit_shas, ['a', 'b'])

    def test_no_commits_raises(self):
        pull = make_pull(False, None, [make_commit('m', ['a', 'main'])])
        with self.assertRaises(ValueError):
            create_pr_source(pull, make_repo(), LOGGER)


class IncompleteNoteTest(unittest.TestCase):
    def test_partial_rebase_walk_sets_note(self):
        pull = make_pull(True, 'r2', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base']),  # association lost mid-series
        )
        source = create_pr_source(pull, repo, LOGGER)
        self.assertIn(f'PR #{PR_NUMBER} has 2 commits', source.incomplete_note)

    def test_confirmed_squash_has_no_note(self):
        pull = make_pull(True, 'merge', [make_commit('a'), make_commit('b', ['a'])])
        merge_commit = make_commit('merge', ['base'])
        merge_commit.commit.message = f'Fix something (#{PR_NUMBER})'
        repo = make_repo(merge_commit, make_commit('base', ['older'], [2]))
        self.assertIsNone(create_pr_source(pull, repo, LOGGER).incomplete_note)

    def test_full_rebase_series_has_no_note(self):
        pull = make_pull(True, 'r2', [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base'], [PR_NUMBER]),
            make_commit('base', ['older'], [2]),
        )
        self.assertIsNone(create_pr_source(pull, repo, LOGGER).incomplete_note)

    def test_single_commit_pr_has_no_note(self):
        pull = make_pull(True, 'merge', [make_commit('a')])
        repo = make_repo(make_commit('merge', ['base']), make_commit('base', ['older'], [2]))
        self.assertIsNone(create_pr_source(pull, repo, LOGGER).incomplete_note)

    def test_skipped_merge_commits_set_note(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('m', ['a', 'main']), make_commit('b', ['m'])])
        source = create_pr_source(pull, make_repo(), LOGGER)
        self.assertIn('1 merge commit(s)', source.incomplete_note)

    def test_no_skipped_merge_commits_no_note(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('b', ['a'])])
        self.assertIsNone(create_pr_source(pull, make_repo(), LOGGER).incomplete_note)


class BuildPrContentTest(unittest.TestCase):
    def make_repo(self):
        repo = mock.Mock()
        repo.full_name = 'ydb-platform/ydb'
        return repo

    def build(self, sources):
        return build_pr_content(
            'ydb-platform/ydb', self.make_repo(), 'token', 'stable', 'stable-dev-branch',
            sources, [], [], 'triggerer', None, None, LOGGER,
        )

    def test_incomplete_note_marks_title_and_body(self):
        source = make_source('PR #1: fix', incomplete_note='PR #1 has 3 commits, but only 1 recovered')
        title, body = self.build([source])
        self.assertTrue(title.startswith('[INCOMPLETE] [Backport stable] '))
        self.assertIn('### ⚠️ Possible incomplete backport', body)
        self.assertIn('- PR #1 has 3 commits, but only 1 recovered', body)

    def test_complete_source_has_no_marker(self):
        title, body = self.build([make_source('PR #1: fix')])
        self.assertFalse(title.startswith('[INCOMPLETE] '))
        self.assertNotIn('Possible incomplete backport', body)

    def test_conflict_marker_combines_with_incomplete(self):
        sources = [make_source('PR #1: fix', incomplete_note='PR #1 has 3 commits')]
        title, _ = build_pr_content(
            'ydb-platform/ydb', self.make_repo(), 'token', 'stable', 'stable-dev-branch',
            sources, [mock.Mock(file_path='a.cpp')], [], 'triggerer', None, None, LOGGER,
        )
        self.assertTrue(title.startswith('[INCOMPLETE] [CONFLICT] [Backport stable] '))


class GetMergedCommitShasTest(unittest.TestCase):
    def test_full_rebase_series(self):
        pull = mock.Mock(number=PR_NUMBER, commits=2)
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base'], [PR_NUMBER]),
        )
        self.assertEqual(get_merged_commit_shas(pull, repo, repo.get_commit('r2'), LOGGER), ['r1', 'r2'])

    def test_walk_stops_when_association_is_lost(self):
        pull = mock.Mock(number=PR_NUMBER, commits=2)
        repo = make_repo(
            make_commit('r2', ['r1'], [PR_NUMBER]),
            make_commit('r1', ['base']),  # not associated with the PR
        )
        self.assertEqual(get_merged_commit_shas(pull, repo, repo.get_commit('r2'), LOGGER), ['r2'])

    def test_api_failure_falls_back_to_merge_commit(self):
        pull = mock.Mock(number=PR_NUMBER, commits=3)
        repo = mock.Mock()
        repo.get_commit.side_effect = GithubException(403, {}, None)
        merge_commit = make_commit('r2', ['r1'], [PR_NUMBER])
        self.assertEqual(get_merged_commit_shas(pull, repo, merge_commit, LOGGER), ['r2'])


class PickLinkedPrTest(unittest.TestCase):
    def test_prefers_merged_pr(self):
        commit = make_commit('c' * 40)
        open_pr = mock.Mock(number=10, merged=False, merged_at=None)
        merged_pr = mock.Mock(number=11, merged=True, merged_at=MERGED_AT)
        commit.get_pulls.return_value = FakePage([open_pr, merged_pr])
        self.assertIs(pick_linked_pr(commit, LOGGER), merged_pr)

    def test_prefers_most_recently_merged_pr(self):
        commit = make_commit('c' * 40)
        older = mock.Mock(number=10, merged=True, merged_at=datetime.datetime(2026, 1, 1, tzinfo=UTC))
        newer = mock.Mock(number=11, merged=True, merged_at=datetime.datetime(2026, 2, 1, tzinfo=UTC))
        commit.get_pulls.return_value = FakePage([newer, older])
        self.assertIs(pick_linked_pr(commit, LOGGER), newer)

    def test_falls_back_to_first_pr_when_none_merged(self):
        commit = make_commit('c' * 40)
        open_pr = mock.Mock(number=10, merged=False, merged_at=None)
        commit.get_pulls.return_value = FakePage([open_pr])
        self.assertIs(pick_linked_pr(commit, LOGGER), open_pr)

    def test_no_prs(self):
        commit = make_commit('c' * 40)
        commit.get_pulls.return_value = FakePage([])
        self.assertIsNone(pick_linked_pr(commit, LOGGER))

    def test_api_failure_returns_none(self):
        commit = make_commit('c' * 40)
        commit.get_pulls.side_effect = GithubException(403, {}, None)
        self.assertIsNone(pick_linked_pr(commit, LOGGER))


class CreateCommitSourceTest(unittest.TestCase):
    def test_merged_linked_pr_uses_pr_merge_time(self):
        source = create_commit_source(make_commit('c' * 40, committer_date=COMMITTED_AT), make_pull(True), LOGGER)
        self.assertTrue(source.is_merged)
        self.assertEqual(source.merged_at, MERGED_AT)

    def test_unmerged_linked_pr(self):
        source = create_commit_source(make_commit('c' * 40, committer_date=COMMITTED_AT), make_pull(False), LOGGER)
        self.assertFalse(source.is_merged)
        self.assertIsNone(source.merged_at)

    def test_no_linked_pr_falls_back_to_committer_date(self):
        source = create_commit_source(make_commit('c' * 40, committer_date=COMMITTED_AT), None, LOGGER)
        self.assertTrue(source.is_merged)
        self.assertEqual(source.merged_at, COMMITTED_AT)


def make_repo_for_commit(commit):
    repo = mock.Mock()
    commits = mock.Mock(totalCount=1)
    commits.__getitem__ = lambda s, i: commit
    repo.get_commits.return_value = commits
    repo.get_commit.return_value = commit
    return repo


class CollectSourcesTest(unittest.TestCase):
    def test_pr_number_resolves_to_pr_source(self):
        pull = make_pull(True, 'merge', [make_commit('a')])
        repo = make_repo(make_commit('merge', ['base']), make_commit('base', ['older'], [2]))
        repo.get_pull.return_value = pull
        sources = collect_sources(repo, ['123'], False, LOGGER)
        self.assertEqual([s.commit_shas for s in sources], [['merge']])
        self.assertTrue(sources[0].is_merged)

    def test_unmerged_pr_without_allow_unmerged_exits(self):
        pull = make_pull(False, None, [make_commit('a')])
        repo = make_repo()
        repo.get_pull.return_value = pull
        with self.assertRaises(SystemExit):
            collect_sources(repo, ['123'], False, LOGGER)

    def test_unmerged_pr_with_allow_unmerged(self):
        pull = make_pull(False, None, [make_commit('a'), make_commit('b', ['a'])])
        repo = make_repo()
        repo.get_pull.return_value = pull
        sources = collect_sources(repo, ['123'], True, LOGGER)
        self.assertEqual(sources[0].commit_shas, ['a', 'b'])
        self.assertFalse(sources[0].is_merged)

    def test_pr_api_error_exits(self):
        repo = mock.Mock()
        repo.get_pull.side_effect = GithubException(404, {}, None)
        with self.assertRaises(SystemExit):
            collect_sources(repo, ['123'], False, LOGGER)

    def test_sha_resolves_to_commit_source(self):
        commit = make_commit('abc123def456', committer_date=COMMITTED_AT)
        commit.get_pulls.return_value = FakePage([])
        sources = collect_sources(make_repo_for_commit(commit), ['abc123'], False, LOGGER)
        self.assertEqual(sources[0].commit_shas, ['abc123def456'])
        self.assertTrue(sources[0].is_merged)
        self.assertEqual(sources[0].merged_at, COMMITTED_AT)

    def test_sha_with_open_and_merged_prs_prefers_merged(self):
        commit = make_commit('abc123def456', committer_date=COMMITTED_AT)
        open_pr = mock.Mock(number=10, merged=False, merged_at=None)
        merged_pr = mock.Mock(number=11, merged=True, merged_at=MERGED_AT)
        commit.get_pulls.return_value = FakePage([open_pr, merged_pr])
        sources = collect_sources(make_repo_for_commit(commit), ['abc123'], False, LOGGER)
        self.assertTrue(sources[0].is_merged)

    def test_sha_with_unmerged_pr_exits(self):
        commit = make_commit('abc123def456')
        commit.get_pulls.return_value = FakePage([mock.Mock(number=10, merged=False, merged_at=None)])
        with self.assertRaises(SystemExit):
            collect_sources(make_repo_for_commit(commit), ['abc123'], False, LOGGER)

    def test_unknown_sha_exits(self):
        repo = mock.Mock()
        repo.get_commits.return_value = mock.Mock(totalCount=0)
        with self.assertRaises(SystemExit):
            collect_sources(repo, ['deadbeef'], False, LOGGER)

    def test_pr_number_and_sha_mixed(self):
        pull = make_pull(True, 'merge', [make_commit('a')])
        commit = make_commit('abc123def456', committer_date=COMMITTED_AT)
        commit.get_pulls.return_value = FakePage([])
        merge_commit = make_commit('merge', ['base'])
        repo = make_repo(merge_commit, make_commit('base', ['older'], [2]))
        repo.get_pull.return_value = pull
        commits = mock.Mock(totalCount=1)
        commits.__getitem__ = lambda s, i: commit
        repo.get_commits.return_value = commits
        by_sha = {c.sha: c for c in [merge_commit, make_commit('base', ['older'], [2]), commit]}
        by_sha[merge_commit.sha] = merge_commit
        repo.get_commit.side_effect = lambda sha: by_sha[sha]
        sources = collect_sources(repo, ['123', 'abc123'], False, LOGGER)
        self.assertEqual([s.type for s in sources], ['pr', 'commit'])


if __name__ == '__main__':
    unittest.main()
