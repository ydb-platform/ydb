import copy
import unittest
from unittest.mock import patch

from sync import GitHub, classify, reconcile


def page(paths, more=False, cursor='next'):
    return {'nodes': [{'path': path} for path in paths],
            'pageInfo': {'hasNextPage': more, 'endCursor': cursor}}


def pr(number=1, paths=(), label=False, more=False):
    return {'number': number, 'state': 'OPEN', 'headRefOid': 'head', 'baseRefOid': 'base',
            'files': page(paths, more),
            'labels': {'nodes': [{'name': 'documentation'}] if label else [],
                       'pageInfo': {'hasNextPage': False}}}


class FakeGitHub:
    has_label = GitHub.has_label

    def __init__(self, prs, pages=()):
        self.prs = copy.deepcopy(prs)
        self.fresh = {p['number']: copy.deepcopy(p) for p in prs}
        self.pages = iter(pages)
        self.writes = []
        self.calls = 0

    def open_prs(self):
        return iter(self.prs)

    def file_pages(self, batch):
        return [next(self.pages) for _ in batch]

    def fresh_pr(self, number):
        return self.fresh[number]

    def set_label(self, number, present):
        self.writes.append((number, present))


class SyncTests(unittest.TestCase):
    def test_add_remove_and_preserve_unrelated_labels(self):
        a = pr(1, ['ydb/docs/ru/page.md'])
        a['labels']['nodes'] = [{'name': 'bug'}]
        b = pr(2, ['ydb/library/a.cpp'], label=True)
        github = FakeGitHub([a, b])
        report = reconcile(github, apply=True)
        self.assertEqual(github.writes, [(1, True), (2, False)])
        self.assertEqual(len(report['changes']), 2)

    def test_dry_run_never_writes(self):
        github = FakeGitHub([pr(paths=['ydb/docs/en/page.md'])])
        self.assertEqual(len(reconcile(github)['changes']), 1)
        self.assertEqual(github.writes, [])

    def test_unchanged_labels_do_not_write(self):
        github = FakeGitHub([pr(paths=['ydb/docs/a'], label=True), pr(2)])
        self.assertEqual(reconcile(github, True)['unchanged'], 2)
        self.assertEqual(github.writes, [])

    def test_exact_current_filename_prefix(self):
        for path in ['ydb/docs-old/file', 'other/ydb/docs/a', 'yql/essentials/docs/a', 'ydb/docs']:
            github = FakeGitHub([pr(paths=[path], label=True)])
            reconcile(github, True)
            self.assertEqual(github.writes, [(1, False)])

    def test_docs_on_later_file_page(self):
        first = pr(paths=['source.cc'], more=True)
        second = dict(first, files=page(['ydb/docs/a.md']))
        github = FakeGitHub([first], [second])
        reconcile(github, True)
        self.assertEqual(github.writes, [(1, True)])

    def test_metadata_scan_then_first_file_page(self):
        metadata = pr()
        del metadata['files']
        files = dict(metadata, files=page(['ydb/docs/a.md']))
        github = FakeGitHub([metadata], [files])
        reconcile(github, True)
        self.assertEqual(github.writes, [(1, True)])

    def test_capped_file_list_preserves_label(self):
        first = pr(paths=['source.cc'] * 3000, label=True)
        github = FakeGitHub([first])
        self.assertEqual(reconcile(github, True)['skipped'], [{'pr': 1, 'reason': 'file-list-limit'}])
        self.assertEqual(github.writes, [])

    def test_docs_in_capped_result_still_add_label(self):
        github = FakeGitHub([pr(paths=['a'] * 2999 + ['ydb/docs/a'])])
        reconcile(github, True)
        self.assertEqual(github.writes, [(1, True)])

    def test_head_or_base_change_during_pagination_is_skipped(self):
        for key in ['headRefOid', 'baseRefOid']:
            first = pr(paths=['source.cc'], more=True, label=True)
            second = dict(first, files=page([]), **{key: 'changed'})
            github = FakeGitHub([first], [second])
            self.assertEqual(reconcile(github, True)['skipped'][0]['reason'], 'changed-during-scan')
            self.assertEqual(github.writes, [])

    def test_changed_or_closed_pr_before_write_is_skipped(self):
        for key, value in [('headRefOid', 'changed'), ('baseRefOid', 'changed'), ('state', 'CLOSED')]:
            github = FakeGitHub([pr(paths=['ydb/docs/a'])])
            github.fresh[1][key] = value
            reconcile(github, True)
            self.assertEqual(github.writes, [])

    def test_current_labels_are_checked_before_write(self):
        github = FakeGitHub([pr(paths=['ydb/docs/a'])])
        github.fresh[1]['labels']['nodes'] = [{'name': 'documentation'}]
        self.assertEqual(reconcile(github, True)['unchanged'], 1)
        self.assertEqual(github.writes, [])

    def test_scan_error_prevents_all_writes(self):
        github = FakeGitHub([pr(paths=['ydb/docs/a']), pr(2, ['code'], more=True)])
        with patch.object(github, 'file_pages', side_effect=RuntimeError('API failure')):
            with self.assertRaises(RuntimeError):
                reconcile(github, True)
        self.assertEqual(github.writes, [])

    def test_parallel_batches_keep_results_attached_to_the_right_pr(self):
        github = FakeGitHub([pr(n, ['code'], more=True) for n in range(1, 24)])
        def pages(batch):
            return [dict(item['pr'], files=page(['ydb/docs/a'] if item['pr']['number'] % 2 else []))
                    for item in batch]
        with patch.object(github, 'file_pages', side_effect=pages):
            reconcile(github, True)
        self.assertEqual(github.writes, [(n, True) for n in range(1, 24, 2)])

    def test_graphql_partial_errors_fail_closed(self):
        github = GitHub('test-placeholder')
        with patch.object(github, 'request', return_value={'data': {}, 'errors': [{'message': 'timeout'}]}):
            with self.assertRaises(RuntimeError):
                github.graphql('query {}')

    def test_label_pagination(self):
        github = GitHub('test-placeholder')
        item = pr()
        item['labels']['pageInfo']['hasNextPage'] = True
        with patch.object(github, 'request', side_effect=[[{'name': 'other'}] * 100, [{'name': 'documentation'}]]):
            self.assertTrue(github.has_label(item))

    def test_mutations_only_touch_documentation_label(self):
        github = GitHub('test-placeholder')
        with patch.object(github, 'request') as request:
            github.set_label(1, True)
            self.assertEqual(request.call_args.args[2], {'labels': ['documentation']})
            github.set_label(1, False)
            self.assertTrue(request.call_args.args[1].endswith('/labels/documentation'))
            self.assertTrue(request.call_args.kwargs['missing_ok'])


if __name__ == '__main__':
    unittest.main()
