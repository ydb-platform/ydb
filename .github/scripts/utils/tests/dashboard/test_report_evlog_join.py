"""Fixture checks for the CI evlog/report join."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import TEST_METRICS, add_product_paths

add_product_paths(TEST_METRICS)

from tests_resource_dashboard import parse_evlog_runs, parse_report_chunks


def test_report_chunk_key_and_evlog_run_match():
    report = {
        "results": [
            {
                "type": "test",
                "path": "ydb/core/kqp",
                "chunk": True,
                "subtest_name": "[1/2] chunk",
                "status": "OK",
                "duration": 2.5,
                "metrics": {"ru_utime": 1.0, "ru_stime": 0.5},
            }
        ]
    }
    evlog = {
        "namespace": "worker_threads",
        "event": "node-finished",
        "thread_name": "worker_1",
        "value": {
            "name": "Run($(BUILD_ROOT)/ydb/core/kqp/test-results/foo/testing_out_stuff/chunk1/run)",
            "time": [1.0, 3.5],
        },
    }
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        report_path = root / "report.json"
        evlog_path = root / "ya_evlog.jsonl"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        evlog_path.write_text(json.dumps(evlog) + "\n", encoding="utf-8")
        chunks, *_rest = parse_report_chunks(report_path, None)
        runs = parse_evlog_runs(evlog_path, None)
    assert ("ydb/core/kqp", None, 1) in chunks
    assert chunks[("ydb/core/kqp", None, 1)]["cpu_sec"] == 1.5
    assert len(runs) == 1
    assert runs[0]["suite_path"] == "ydb/core/kqp"
    assert runs[0]["chunk"] == 1
    assert runs[0]["dur_us"] == 2_500_000.0


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTest(unittest.FunctionTestCase(test_report_chunk_key_and_evlog_run_match))
    return suite


if __name__ == "__main__":
    test_report_chunk_key_and_evlog_run_match()
    print("OK")
