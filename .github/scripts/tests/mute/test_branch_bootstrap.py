#!/usr/bin/env python3
import datetime
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

_MUTE_DIR = Path(__file__).resolve().parent
_TESTS_DIR = _MUTE_DIR.parent
_SCRIPTS_DIR = _TESTS_DIR.parent
for _p in (_TESTS_DIR, _SCRIPTS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from mute.branch_bootstrap import (  # noqa: E402
    apply_branch_bootstrap_grace,
    git_branch_first_seen_in_branches_config,
    is_branch_bootstrap_grace_active,
    load_inherited_muted_lines,
)


class BranchBootstrapGitTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.config_rel = '.github/config/stable_tests_branches.json'
        self.config_path = self.root / self.config_rel
        self.config_path.parent.mkdir(parents=True)
        subprocess.run(['git', 'init'], cwd=self.root, check=True, capture_output=True)
        subprocess.run(
            ['git', 'config', 'user.email', 'test@example.com'],
            cwd=self.root,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ['git', 'config', 'user.name', 'Test User'],
            cwd=self.root,
            check=True,
            capture_output=True,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def _commit_config(self, branches, date_env: str):
        self.config_path.write_text(json.dumps(branches, indent=4) + '\n', encoding='utf-8')
        env = os.environ.copy()
        env['GIT_AUTHOR_DATE'] = date_env
        env['GIT_COMMITTER_DATE'] = date_env
        subprocess.run(['git', 'add', self.config_rel], cwd=self.root, check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', f'update branches: {branches}'],
            cwd=self.root,
            check=True,
            capture_output=True,
            env=env,
        )

    def test_first_seen_from_git_history(self):
        self._commit_config(['main'], '2026-01-01T12:00:00+00:00')
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')

        seen = git_branch_first_seen_in_branches_config(
            'stable-26-3',
            branches_config_rel=self.config_rel,
            repo_root=str(self.root),
        )
        self.assertEqual(seen, datetime.date(2026, 7, 10))

    def test_grace_active_within_seven_days(self):
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')

        active, added = is_branch_bootstrap_grace_active(
            'stable-26-3',
            today=datetime.date(2026, 7, 16),
            branches_config_rel=self.config_rel,
            repo_root=str(self.root),
        )
        self.assertTrue(active)
        self.assertEqual(added, datetime.date(2026, 7, 10))

    def test_grace_inactive_after_seven_days(self):
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')

        active, added = is_branch_bootstrap_grace_active(
            'stable-26-3',
            today=datetime.date(2026, 7, 17),
            branches_config_rel=self.config_rel,
            repo_root=str(self.root),
        )
        self.assertFalse(active)
        self.assertEqual(added, datetime.date(2026, 7, 10))

    def test_main_never_in_bootstrap(self):
        self._commit_config(['main'], '2026-07-10T10:00:00+00:00')
        seen = git_branch_first_seen_in_branches_config(
            'main',
            branches_config_rel=self.config_rel,
            repo_root=str(self.root),
        )
        self.assertIsNone(seen)

    def test_apply_during_grace_merges_inherited_and_filters_delete(self):
        self._commit_config(['main', 'stable-26-3'], '2026-07-10T10:00:00+00:00')
        muted = self.root / 'muted_ya.txt'
        muted.write_text('suite test\n', encoding='utf-8')

        all_muted, to_delete, active, added = apply_branch_bootstrap_grace(
            branch='stable-26-3',
            inherited_muted_ya_path=str(muted),
            all_muted_ya=[],
            to_delete=['suite test'],
            today=datetime.date(2026, 7, 12),
            branches_config_rel=self.config_rel,
            repo_root=str(self.root),
        )
        self.assertTrue(active)
        self.assertEqual(all_muted, ['suite test'])
        self.assertEqual(to_delete, [])
        self.assertEqual(added, datetime.date(2026, 7, 10))


class BranchBootstrapApplyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.muted_path = Path(self._tmp.name) / 'muted_ya.txt'
        self.muted_path.write_text(
            'ydb/tests/functional/suite test_case\n'
            'ydb/tests/other suite other_test\n',
            encoding='utf-8',
        )

    def tearDown(self):
        self._tmp.cleanup()

    def test_apply_preserves_inherited_and_skips_delete(self):
        inherited = load_inherited_muted_lines(str(self.muted_path))
        all_muted, to_delete, active, added = apply_branch_bootstrap_grace(
            branch='stable-26-3',
            inherited_muted_ya_path=str(self.muted_path),
            all_muted_ya=[],
            to_delete=['ydb/tests/functional/suite test_case'],
            today=datetime.date(2026, 7, 12),
            branches_config_rel='.github/config/stable_tests_branches.json',
            repo_root='/nonexistent',
        )
        # Without git history grace is inactive — verify baseline.
        self.assertFalse(active)
        self.assertEqual(all_muted, [])
        self.assertEqual(to_delete, ['ydb/tests/functional/suite test_case'])
        self.assertEqual(len(inherited), 2)


if __name__ == '__main__':
    unittest.main()
