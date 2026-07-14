#!/usr/bin/env python3
"""Unit tests for the "new stable branch grace" feature in ``create_new_muted_ya.py``.

Covers:
- ``_git_branch_added_to_stable_config``: date derivation from git history via
  ``git log -S`` (pickaxe), ``main`` exclusion, "branch never added" case.
- ``_apply_stable_branch_grace``: inherited-mute preservation during the grace
  window, expiry after the window, and — the actual bug this suite guards against —
  that the ``*_debug`` lists it returns stay 1:1 in sync with their raw counterparts
  (``to_delete``/``to_delete_debug`` and ``all_muted_ya``/``all_muted_ya_debug``).
- ``apply_and_add_mutes`` end-to-end: the files written to disk for ``to_delete.txt``
  and ``muted_ya.txt`` have exactly as many lines as their ``_debug.txt`` siblings.

Run directly (works from any cwd): ``python3 .github/scripts/tests/mute/tests/test_stable_branch_grace.py``
or via discovery from the ``mute`` dir: ``cd .github/scripts/tests/mute && python3 -m unittest discover -s tests``.
(Running ``-m unittest`` from the repo root adds the repo root to ``sys.path``, which shadows
the installed ``ydb`` package with the in-repo ``ydb/`` source tree — run from ``mute/`` instead.)
"""
import datetime
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MUTE_DIR = _HERE.parent
_TESTS_DIR = _MUTE_DIR.parent
_SCRIPTS_DIR = _TESTS_DIR.parent
for _p in (str(_MUTE_DIR), str(_TESTS_DIR), str(_SCRIPTS_DIR), str(_SCRIPTS_DIR / 'analytics')):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import create_new_muted_ya as cnm  # noqa: E402


def _git(args, cwd, env=None):
    subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, env=env)


class _GitConfigRepoMixin:
    """Builds a throwaway git repo with a commit history for stable_tests_branches.json."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.config_rel = cnm._STABLE_BRANCHES_CONFIG
        self.config_path = self.root / self.config_rel
        self.config_path.parent.mkdir(parents=True)
        _git(['init', '-q'], cwd=self.root)
        _git(['config', 'user.email', 'test@example.com'], cwd=self.root)
        _git(['config', 'user.name', 'Test User'], cwd=self.root)

    def tearDown(self):
        self._tmp.cleanup()

    def _commit_config(self, branches, date_iso):
        self.config_path.write_text(json.dumps(branches, indent=4) + '\n', encoding='utf-8')
        env = os.environ.copy()
        env['GIT_AUTHOR_DATE'] = date_iso
        env['GIT_COMMITTER_DATE'] = date_iso
        _git(['add', self.config_rel], cwd=self.root)
        _git(['commit', '-q', '-m', f'update branches: {branches}'], cwd=self.root, env=env)

    def _write_inherited(self, lines):
        path = self.root / 'muted_ya.txt'
        path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        return str(path)


class GitBranchAddedToStableConfigTest(_GitConfigRepoMixin, unittest.TestCase):
    def test_first_seen_from_git_history(self):
        self._commit_config(['main'], '2026-01-01T12:00:00+00:00')
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')

        added = cnm._git_branch_added_to_stable_config('stable-26-3', str(self.root))
        self.assertEqual(added, datetime.date(2026, 7, 10))

    def test_main_is_never_dated(self):
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')
        self.assertIsNone(cnm._git_branch_added_to_stable_config('main', str(self.root)))

    def test_branch_never_added_returns_none(self):
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')
        added = cnm._git_branch_added_to_stable_config('stable-does-not-exist', str(self.root))
        self.assertIsNone(added)

    def test_no_branch_argument_returns_none(self):
        self.assertIsNone(cnm._git_branch_added_to_stable_config('', str(self.root)))
        self.assertIsNone(cnm._git_branch_added_to_stable_config(None, str(self.root)))

    def test_multiple_config_edits_still_finds_first_addition(self):
        # Several unrelated edits before and after the branch is added must not
        # confuse the pickaxe fast path into picking the wrong commit.
        self._commit_config(['main'], '2026-01-01T00:00:00+00:00')
        self._commit_config(['main', 'stable-25-3'], '2026-02-01T00:00:00+00:00')
        self._commit_config(['main', 'stable-25-3', 'stable-25-4'], '2026-03-01T00:00:00+00:00')
        self._commit_config(
            ['main', 'stable-25-3', 'stable-25-4', 'stable-26-1'], '2026-04-01T00:00:00+00:00'
        )
        self._commit_config(
            ['main', 'stable-25-4', 'stable-26-1'], '2026-05-01T00:00:00+00:00'
        )  # stable-25-3 removed

        self.assertEqual(
            cnm._git_branch_added_to_stable_config('stable-26-1', str(self.root)),
            datetime.date(2026, 4, 1),
        )
        self.assertEqual(
            cnm._git_branch_added_to_stable_config('stable-25-4', str(self.root)),
            datetime.date(2026, 3, 1),
        )

    def test_missing_repo_root_does_not_raise(self):
        # No git repo at all at this path: git will fail (non-zero exit), the function
        # must degrade to None rather than raising.
        added = cnm._git_branch_added_to_stable_config('stable-26-3', str(self.root / 'nope'))
        self.assertIsNone(added)


def _debug_prefixes(debug_lines):
    return {cnm._debug_line_test_string(d) for d in debug_lines}


class ApplyStableBranchGraceTest(_GitConfigRepoMixin, unittest.TestCase):
    def test_inactive_when_branch_never_added(self):
        self._commit_config(['main'], '2026-01-01T00:00:00+00:00')
        inherited_path = self._write_inherited(['suite testA'])
        result = cnm._apply_stable_branch_grace(
            'stable-unknown', inherited_path, ['x'], ['x # debug'], ['y'], ['y # debug'], str(self.root)
        )
        all_muted_ya, all_muted_ya_debug, to_delete, to_delete_debug, grace_inherited, since, until = result
        self.assertEqual(all_muted_ya, ['x'])
        self.assertEqual(to_delete, ['y'])
        self.assertEqual(grace_inherited, frozenset())
        self.assertIsNone(since)
        self.assertIsNone(until)

    def test_inactive_after_grace_window_expires(self):
        self._commit_config(['main', 'stable-26-3'], '2020-01-01T00:00:00+00:00')
        inherited_path = self._write_inherited(['suite testA'])
        result = cnm._apply_stable_branch_grace(
            'stable-26-3', inherited_path, [], [], ['suite testA'], ['suite testA # debug'], str(self.root)
        )
        all_muted_ya, all_muted_ya_debug, to_delete, to_delete_debug, grace_inherited, since, until = result
        # Long expired -> untouched passthrough.
        self.assertEqual(to_delete, ['suite testA'])
        self.assertEqual(grace_inherited, frozenset())

    def test_active_grace_protects_delete_and_keeps_debug_counts_in_sync(self):
        today = datetime.datetime.now(datetime.timezone.utc).date()
        self._commit_config(['main', 'stable-26-3'], today.isoformat() + 'T00:00:00+00:00')

        # "suiteA testA" has zero monitor runs -> normally a delete candidate, and is
        # also present in the inherited file, so grace must protect it.
        # "suiteB testB" is inherited but never showed up in monitor data at all (the
        # classic brand-new-branch case) -> grace must add it to muted_ya from scratch.
        inherited_path = self._write_inherited(['suiteA testA', 'suiteB testB'])

        all_muted_ya = ['suiteA testA']
        all_muted_ya_debug = ['suiteA testA # owner o success_rate 0% [2026-01-01], p-0, f-0,m-0, s-0, runs-0, mute state: muted, test state: Muted']
        to_delete = ['suiteA testA']
        to_delete_debug = list(all_muted_ya_debug)

        (
            new_all_muted_ya,
            new_all_muted_ya_debug,
            new_to_delete,
            new_to_delete_debug,
            grace_inherited,
            since,
            until,
        ) = cnm._apply_stable_branch_grace(
            'stable-26-3', inherited_path, all_muted_ya, all_muted_ya_debug, to_delete, to_delete_debug,
            str(self.root),
        )

        self.assertEqual(since, today)
        self.assertEqual(until, today + datetime.timedelta(days=cnm.get_stable_branch_grace_days() - 1))
        self.assertEqual(grace_inherited, frozenset({'suiteA testA', 'suiteB testB'}))

        # Bug regression check #1: to_delete must no longer contain the protected test,
        # and to_delete_debug must be trimmed to match (same length, same test coverage).
        self.assertEqual(new_to_delete, [])
        self.assertEqual(len(new_to_delete), len(new_to_delete_debug))
        self.assertEqual(new_to_delete_debug, [])

        # Bug regression check #2: muted_ya gains suiteB testB, and muted_ya_debug must
        # gain a matching line for it too (line counts must stay equal).
        self.assertEqual(new_all_muted_ya, ['suiteA testA', 'suiteB testB'])
        self.assertEqual(len(new_all_muted_ya), len(new_all_muted_ya_debug))
        self.assertEqual(_debug_prefixes(new_all_muted_ya_debug), {'suiteA testA', 'suiteB testB'})
        # The pre-existing debug line for suiteA testA (real monitor data) must be
        # preserved untouched, not replaced by the generic grace placeholder.
        self.assertIn(all_muted_ya_debug[0], new_all_muted_ya_debug)

    def test_grace_does_not_resurrect_legitimate_unmute(self):
        # Grace only protects against zero-run delete; it must not fight the unmute
        # rule when a test genuinely has enough passing runs on the new branch.
        today = datetime.datetime.now(datetime.timezone.utc).date()
        self._commit_config(['main', 'stable-26-3'], today.isoformat() + 'T00:00:00+00:00')
        inherited_path = self._write_inherited(['suiteA testA'])

        all_muted_ya = ['suiteA testA']
        all_muted_ya_debug = ['suiteA testA # owner o success_rate 100% [2026-01-01], p-5, f-0,m-0, s-0, runs-5, mute state: muted, test state: Active']
        to_delete = []
        to_delete_debug = []

        result = cnm._apply_stable_branch_grace(
            'stable-26-3', inherited_path, all_muted_ya, all_muted_ya_debug, to_delete, to_delete_debug,
            str(self.root),
        )
        new_all_muted_ya = result[0]
        # all_muted_ya still contains it (grace only ever adds/keeps); the caller's
        # separate to_unmute computation (outside this function) is what actually
        # drops it from the final muted_ya-to_unmute output.
        self.assertEqual(new_all_muted_ya, ['suiteA testA'])


class ApplyAndAddMutesEndToEndTest(_GitConfigRepoMixin, unittest.TestCase):
    """Exercise apply_and_add_mutes() end-to-end and check the files written to disk."""

    def _read_lines(self, path):
        if not os.path.exists(path):
            return []
        with open(path, encoding='utf-8') as fp:
            return [line.rstrip('\n') for line in fp]

    def test_to_delete_and_muted_ya_files_stay_paired_with_their_debug_files(self):
        today = datetime.datetime.now(datetime.timezone.utc).date()
        self._commit_config(['main', 'stable-x'], today.isoformat() + 'T00:00:00+00:00')

        inherited_path = self._write_inherited(['suiteA testA', 'suiteB testB'])

        all_data = [
            {
                'test_name': 'testA', 'suite_folder': 'suiteA', 'full_name': 'suiteA/testA',
                'build_type': 'relwithdebinfo', 'branch': 'stable-x',
                'date_window': today,
                'pass_count': 0, 'fail_count': 0, 'mute_count': 0, 'skip_count': 0,
                'owner': 'o', 'is_muted': True, 'state': 'Muted', 'days_in_state': 1,
                'is_test_chunk': 0,
            },
        ]
        aggregated_for_delete = [dict(all_data[0], period_days=7)]

        out_dir = self.root / 'out'
        out_dir.mkdir()

        cnm.apply_and_add_mutes(
            all_data,
            str(out_dir),
            mute_check=lambda suite, case: True,
            aggregated_for_mute=[],
            aggregated_for_unmute=[],
            aggregated_for_delete=aggregated_for_delete,
            branch='stable-x',
            build_type='relwithdebinfo',
            inherited_muted_ya_path=inherited_path,
            repo_root=str(self.root),
        )

        mute_update_dir = out_dir / 'mute_update'
        to_delete = self._read_lines(mute_update_dir / 'to_delete.txt')
        to_delete_debug = self._read_lines(mute_update_dir / 'to_delete_debug.txt')
        muted_ya = self._read_lines(mute_update_dir / 'muted_ya.txt')
        muted_ya_debug = self._read_lines(mute_update_dir / 'muted_ya_debug.txt')

        # Grace must have protected suiteA/testA from deletion and pulled in suiteB/testB.
        self.assertEqual(to_delete, [])
        self.assertEqual(sorted(muted_ya), ['suiteA testA', 'suiteB testB'])

        # The actual bug this test guards against: line counts (and thus the
        # "Removed from mute" / muted_ya PR-body sections built from *_debug.txt)
        # must stay 1:1 with their .txt counterparts.
        self.assertEqual(len(to_delete), len(to_delete_debug))
        self.assertEqual(len(muted_ya), len(muted_ya_debug))


if __name__ == '__main__':
    unittest.main()
