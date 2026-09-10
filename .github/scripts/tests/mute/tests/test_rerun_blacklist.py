#!/usr/bin/env python3
"""Tests for the rerun blacklist built by ``mute_utils.convert_muted_txt_to_yaml``.

Run from ``.github/scripts/tests/mute``: ``python3 -m unittest discover -s tests``
(running from the repo root shadows the installed ``ydb`` package).
"""
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
_MUTE_DIR = _HERE.parent
_TESTS_DIR = _MUTE_DIR.parent
_SCRIPTS_DIR = _TESTS_DIR.parent
for _p in (str(_TESTS_DIR), str(_SCRIPTS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from mute.mute_utils import convert_muted_txt_to_yaml  # noqa: E402

DATASTREAMS = 'ydb/core/kqp/ut/federated_query/datastreams'
OLTP = 'ydb/tests/stress/oltp_workload/tests'


def failed(path, name, subtest_name=''):
    return {'path': path, 'name': name, 'subtest_name': subtest_name, 'status': 'FAILED'}


def muted(path, name, subtest_name=''):
    return {'path': path, 'name': name, 'subtest_name': subtest_name, 'status': 'MUTE', 'muted': True}


def skipped(path, name, subtest_name=''):
    return {'path': path, 'name': name, 'subtest_name': subtest_name, 'status': 'SKIPPED'}


def passed(path, name, subtest_name=''):
    return {'path': path, 'name': name, 'subtest_name': subtest_name, 'status': 'PASSED'}


class ConvertMutedTxtToYamlTest(unittest.TestCase):
    def convert(self, muted_lines, results):
        with tempfile.TemporaryDirectory() as tmp:
            muted_txt = Path(tmp) / 'muted_ya.txt'
            muted_txt.write_text('\n'.join(muted_lines) + '\n')
            report = Path(tmp) / 'report.json'
            report.write_text(json.dumps({'results': results}))

            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                convert_muted_txt_to_yaml(str(muted_txt), str(report))
        return yaml.safe_load(out.getvalue()) or []

    def filters_by_path(self, muted_lines, results):
        return {entry['path']: entry.get('test_filter') for entry in self.convert(muted_lines, results)}

    def test_suite_with_only_muted_failures_is_blacklisted(self):
        # Nothing else to rerun in the suite, so keep it out of the rerun.
        filters = self.filters_by_path(
            [f'{DATASTREAMS} KqpStreamingQueriesDdl.CreateAndAlterStreamingQuery'],
            [
                muted(DATASTREAMS, 'KqpStreamingQueriesDdl', 'CreateAndAlterStreamingQuery'),
                passed(DATASTREAMS, 'KqpStreamingQueriesDdl', 'CreateAndDropStreamingQuery'),
            ],
        )
        self.assertEqual(filters, {DATASTREAMS: 'KqpStreamingQueriesDdl::CreateAndAlterStreamingQuery'})

    def test_unittest_suite_with_unmuted_failure_is_not_blacklisted(self):
        # PR 50378: 4 crashed tests next to a muted one. Blacklisting the path made ya
        # drop the whole suite, so the crashes never reran and the check went green.
        filters = self.filters_by_path(
            [f'{DATASTREAMS} KqpStreamingQueriesDdl.CreateAndAlterStreamingQuery'],
            [
                muted(DATASTREAMS, 'KqpStreamingQueriesDdl', 'CreateAndAlterStreamingQuery'),
                failed(DATASTREAMS, 'KqpStreamingQueriesDdl', 'StreamingQueryWithStreamLookupJoinLocalTable+WithFeatureFlag'),
            ],
        )
        self.assertEqual(filters, {})

    def test_pytest_suite_with_unmuted_failure_is_not_blacklisted(self):
        # PR 49067: muted TestYdbWorkload.test next to a failing test_tli. The blacklist
        # narrowed the suite down to a placeholder name that matches no real test.
        filters = self.filters_by_path(
            [f'{OLTP} test_workload.py.TestYdbWorkload.test'],
            [
                muted(OLTP, 'test_workload.py', 'TestYdbWorkload.test'),
                failed(OLTP, 'test_workload.py', 'TestYdbWorkload.test_tli'),
            ],
        )
        self.assertEqual(filters, {})

    def test_unmuted_failure_only_affects_its_own_suite(self):
        filters = self.filters_by_path(
            [
                f'{DATASTREAMS} KqpStreamingQueriesDdl.CreateAndAlterStreamingQuery',
                f'{OLTP} test_workload.py.TestYdbWorkload.test',
            ],
            [
                muted(DATASTREAMS, 'KqpStreamingQueriesDdl', 'CreateAndAlterStreamingQuery'),
                failed(DATASTREAMS, 'KqpStreamingQueriesDdl', 'StreamingQueryWithStreamLookupJoinLocalTable+WithFeatureFlag'),
                muted(OLTP, 'test_workload.py', 'TestYdbWorkload.test'),
            ],
        )
        self.assertEqual(filters, {OLTP: 'test_workload.py::TestYdbWorkload::test'})

    def test_error_status_counts_as_unmuted_failure(self):
        filters = self.filters_by_path(
            [f'{DATASTREAMS} KqpStreamingQueriesDdl.CreateAndAlterStreamingQuery'],
            [{'path': DATASTREAMS, 'name': 'KqpStreamingQueriesDdl', 'subtest_name': 'Other', 'status': 'ERROR'}],
        )
        self.assertEqual(filters, {})

    def test_chunk_lines_and_previously_skipped_tests_are_still_dropped(self):
        filters = self.filters_by_path(
            [
                f'{DATASTREAMS} unittest.[*/*] chunk',
                f'{DATASTREAMS} KqpStreamingQueriesDdl.CreateAndAlterStreamingQuery',
                f'{OLTP} test_workload.py.TestYdbWorkload.test',
            ],
            [
                muted(DATASTREAMS, 'KqpStreamingQueriesDdl', 'CreateAndAlterStreamingQuery'),
                skipped(OLTP, 'test_workload.py', 'TestYdbWorkload.test'),
            ],
        )
        self.assertEqual(filters, {DATASTREAMS: 'KqpStreamingQueriesDdl::CreateAndAlterStreamingQuery'})


if __name__ == '__main__':
    unittest.main()
