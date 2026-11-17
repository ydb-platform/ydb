import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name('sync.py')


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.git('init', '-q')
        self.git('config', 'user.email', 'test@example.com')
        self.git('config', 'user.name', 'Test')
        self.src = 'yql/essentials/docs/ru'
        self.dst = 'ydb/docs/ru/core/yql/reference'
        self.config = 'ydb/docs/tools/yql_sync/ownership.json'
        for name in ['plain.md', 'edited.md', 'reverted.md']:
            self.write(f'{self.src}/{name}', '# Original\n')
            self.write(f'{self.dst}/{name}', '# Original\n')
        self.write(f'{self.src}/toc_p.yaml', 'items: []\n')
        self.write(f'{self.dst}/toc_p.yaml', 'items: []\n')
        self.base = self.commit()
        self.write(f'{self.dst}/essentials.base_rev', self.base+';'+self.src+'\n')
        self.write(f'{self.dst}/edited.md', '# YDB\n')
        self.write(f'{self.dst}/local.md', '# Local\n')
        self.commit()
        spec = importlib.util.spec_from_file_location('sync', SCRIPT)
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)
        self.mod.initialize(self.root, self.config, ['ru'])
        self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.root), *args], stderr=subprocess.PIPE).decode().strip()

    def write(self, path, text):
        p = self.root/path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def commit(self):
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def state(self):
        return json.loads((self.root/self.config).read_text())

    def apply(self):
        return self.mod.synchronize(self.root, self.config, apply=True)

    def test_imports_only_untouched_files_and_is_idempotent(self):
        for name in ['plain.md', 'edited.md']:
            self.write(f'{self.src}/{name}', '# Updated\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Updated\n')
        self.assertEqual((self.root/self.dst/'edited.md').read_text(), '# YDB\n')
        self.assertEqual(self.state()['files'][f'{self.dst}/local.md']['owner'], 'ydb')
        self.commit()
        self.apply()
        self.assertEqual(self.git('status', '--porcelain'), '')
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'yql')

    def test_second_sync_does_not_claim_the_first_sync_as_local(self):
        for contents in ['# First upstream update\n', '# Second upstream update\n']:
            self.write(f'{self.src}/plain.md', contents)
            self.commit()
            self.apply()
            self.assertEqual((self.root/self.dst/'plain.md').read_text(), contents)
            self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'yql')
            self.commit()

    def test_local_edit_matching_upstream_still_transfers_ownership(self):
        self.write(f'{self.dst}/plain.md', '# Same change\n')
        self.write(f'{self.src}/plain.md', '# Same change\n')
        self.commit()
        self.apply()
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'ydb')

    def test_local_edit_then_revert_permanently_claims_file(self):
        self.write(f'{self.dst}/plain.md', '# Local edit\n')
        self.commit()
        self.write(f'{self.dst}/plain.md', '# Original\n')
        self.commit()
        self.write(f'{self.src}/plain.md', '# Updated\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Original\n')
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'ydb')
        self.commit()
        self.write(f'{self.src}/plain.md', '# Another update\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Original\n')

    def test_deleted_or_renamed_destination_is_never_recreated(self):
        self.git('mv', f'{self.dst}/plain.md', f'{self.dst}/renamed.md')
        self.git('rm', f'{self.dst}/reverted.md')
        self.commit()
        self.apply()
        self.assertFalse((self.root/self.dst/'plain.md').exists())
        self.assertFalse((self.root/self.dst/'reverted.md').exists())
        self.assertEqual(self.state()['files'][f'{self.dst}/renamed.md']['owner'], 'ydb')

    def test_merge_preserves_ownership_after_branch_edit_and_revert(self):
        main = self.git('branch', '--show-current')
        self.git('checkout', '-qb', 'local-edit')
        self.write(f'{self.dst}/plain.md', '# Branch edit\n')
        self.commit()
        self.write(f'{self.dst}/plain.md', '# Original\n')
        self.commit()
        self.git('checkout', main)
        self.git('merge', '--no-ff', '-m', 'Merge local edits', 'local-edit')
        self.write(f'{self.src}/plain.md', '# Updated\n')
        self.commit()
        self.apply()
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'ydb')
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Original\n')

    def test_merge_of_sync_branch_remains_eligible(self):
        main = self.git('branch', '--show-current')
        self.write(f'{self.src}/plain.md', '# Updated\n')
        self.commit()
        self.git('checkout', '-qb', 'sync-update')
        self.apply()
        self.commit()
        self.git('checkout', main)
        self.git('merge', '--no-ff', '-m', 'Merge sync', 'sync-update')
        self.apply()
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'yql')
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_upstream_new_and_deleted_files_do_not_change_target_tree(self):
        self.git('rm', f'{self.src}/plain.md')
        self.write(f'{self.src}/new.md', '# New\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Original\n')
        self.assertFalse((self.root/self.dst/'new.md').exists())
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'ydb')

    def test_dry_run_changes_nothing(self):
        self.write(f'{self.src}/plain.md', '# Updated\n')
        self.commit()
        report = self.mod.synchronize(self.root, self.config, apply=False)
        self.assertTrue(report['updated'])
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_dirty_tree_is_rejected_without_overwriting_anything(self):
        self.write(f'{self.dst}/plain.md', '# Uncommitted\n')
        before = (self.root/self.config).read_bytes()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Uncommitted\n')
        self.assertEqual((self.root/self.config).read_bytes(), before)

    def test_sync_commit_followed_by_local_edit_is_detected(self):
        self.write(f'{self.src}/plain.md', '# Updated\n')
        self.commit()
        self.apply()
        self.commit()
        self.write(f'{self.dst}/plain.md', '# YDB after sync\n')
        self.commit()
        self.write(f'{self.src}/plain.md', '# More upstream\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# YDB after sync\n')
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'ydb')

    def test_legacy_script_selective_entry_runs_the_new_sync(self):
        helper = self.root/'ydb/docs/tools/yql_sync/sync.py'
        helper.write_bytes(SCRIPT.read_bytes())
        shell = self.root/'yql/essentials/docs/scripts/doc_upgrade_git.sh'
        shell.parent.mkdir(parents=True, exist_ok=True)
        shell.write_bytes((SCRIPT.parents[4]/'yql/essentials/docs/scripts/doc_upgrade_git.sh').read_bytes())
        self.commit()
        self.write(f'{self.src}/plain.md', '# Via shell entry\n')
        self.commit()
        run = subprocess.run(['bash', str(shell), '--selective', '--apply'], cwd=self.root,
                             capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Via shell entry\n')

    def test_initial_source_revision_need_not_be_a_main_ancestor(self):
        orphan = self.git('commit-tree', self.git('rev-parse', self.base+'^{tree}'), '-m', 'exported source')
        (self.root/self.config).unlink()
        self.write(f'{self.dst}/essentials.base_rev', orphan+';'+self.src+'\n')
        self.commit()
        self.mod.initialize(self.root, self.config, ['ru'])
        self.assertEqual(self.state()['files'][f'{self.dst}/plain.md']['owner'], 'yql')

    def test_missing_history_fails_closed(self):
        state = self.state()
        state['checkpoint'] = '0'*40
        self.write(self.config, json.dumps(state))
        self.commit()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_path_traversal_is_rejected_before_any_write(self):
        state = self.state()
        state['files']['../escape.md'] = state['files'].pop(f'{self.dst}/plain.md')
        self.write(self.config, json.dumps(state))
        self.commit()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertFalse((self.root.parent/'escape.md').exists())

    def test_symlink_source_is_not_followed(self):
        p=self.root/self.src/'plain.md'
        p.unlink()
        p.symlink_to(self.root/self.dst/'local.md')
        self.commit()
        with self.assertRaises(ValueError):
            self.apply()
        self.assertEqual((self.root/self.dst/'plain.md').read_text(), '# Original\n')

    def test_topology_and_assets_remain_owned(self):
        self.write(f'{self.src}/toc_p.yaml', 'items: [new]\n')
        self.commit()
        self.apply()
        self.assertEqual((self.root/self.dst/'toc_p.yaml').read_text(), 'items: []\n')


if __name__ == '__main__':
    unittest.main()
