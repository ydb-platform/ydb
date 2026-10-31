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

from tests_resource_dashboard import (
    _build_resources_overlay,
    parse_evlog_activity,
    parse_evlog_activity_deltas,
    parse_evlog_runs,
    parse_report_chunks,
)
from dashboard_html_payload import build_dashboard_payload
from dashboard_html_series import downsample_active_series


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


def test_suite_rows_are_not_tests_and_later_chunk_finish_wins():
    report = {
        "results": [
            {"type": "test", "path": "ydb/core/kqp", "suite": True, "subtest_name": "suite", "status": "OK"},
            {
                "type": "test",
                "path": "ydb/core/kqp",
                "chunk": True,
                "subtest_name": "[1/1] chunk",
                "status": "OK",
                "metrics": {"suite_finish_timestamp": 10},
            },
            {"type": "test", "path": "ydb/core/kqp", "subtest_name": "test_a", "status": "OK", "duration": 1},
            {
                "type": "test",
                "path": "ydb/core/kqp",
                "chunk": True,
                "subtest_name": "[1/1] chunk",
                "status": "FAIL",
                "metrics": {"suite_finish_timestamp": 20},
            },
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        report_path = Path(tmp) / "report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        chunks, _status, _per_chunk, tests_per_suite, *_rest, suite_chunk_issues = parse_report_chunks(
            report_path, None
        )
    assert tests_per_suite["ydb/core/kqp"] == 1
    assert chunks[("ydb/core/kqp", None, 1)]["status"] == "FAIL"
    assert suite_chunk_issues["duplicate_chunk_rows"] == 1


def test_evlog_activity_covers_compile_link_and_test_jobs():
    events = [
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "FromDistCache(hash)", "time": [5, 15]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "Compile(a.cpp)", "time": [10, 20]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "Archive(lib.a)", "time": [12, 18]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {"name": "Link(lib.a)", "time": [15, 30]},
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {
                "name": "Run(hash$(BUILD_ROOT)/bin/executable)",
                "time": [16, 31],
                "tag": "LD",
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {
                "name": "Run(hash$(BUILD_ROOT)/lib/archive.a)",
                "time": [17, 32],
                "tag": "AR",
            },
        },
        {
            "namespace": "worker_threads",
            "event": "node-finished",
            "value": {
                "name": "Run(rnd-a$(BUILD_ROOT)/ydb/core/kqp/test-results/foo/testing_out_stuff/chunk1/run)",
                "time": [25, 40],
                "tag": "TM",
            },
        },
    ]
    with tempfile.TemporaryDirectory() as tmp:
        evlog_path = Path(tmp) / "ya_evlog.jsonl"
        evlog_path.write_text(
            "".join(json.dumps(event) + "\n" for event in events),
            encoding="utf-8",
        )
        activity = parse_evlog_activity(evlog_path)
    kinds = [row["kind"] for row in activity]
    assert kinds.count("cache") == 1
    assert kinds.count("build") == 1
    assert kinds.count("archive") == 2
    assert kinds.count("link") == 2
    assert kinds.count("test_tm") == 1
    assert activity[0]["start_us"] == 5_000_000.0
    assert activity[-1]["end_us"] == 40_000_000.0


def test_evlog_activity_chrome_trace_uses_event_name_as_tag():
    events = [
        {
            "ph": "B",
            "pid": 1,
            "tid": 1,
            "ts": 100.0,
            "name": "TM",
            "args": {
                "name": "Run(rnd-aaa$(BUILD_ROOT)/ydb/core/kqp/test-results/foo/testing_out_stuff/chunk1/run)"
            },
        },
        {"ph": "E", "pid": 1, "tid": 1, "ts": 250.0},
        {
            "ph": "B",
            "pid": 1,
            "tid": 2,
            "ts": 110.0,
            "name": "AR",
            "args": {"name": "Run(hash$(BUILD_ROOT)/lib/x.a)"},
        },
        {"ph": "E", "pid": 1, "tid": 2, "ts": 200.0},
    ]
    activity = parse_evlog_activity(events=events)
    kinds = [row["kind"] for row in activity]
    assert kinds == ["test_tm", "archive"]
    assert activity[0]["start_us"] == 100.0
    assert activity[0]["end_us"] == 250.0


def test_evlog_activity_closes_tm_on_result_without_e():
    events = [
        {
            "ph": "B",
            "pid": 1,
            "tid": 7,
            "ts": 10.0,
            "name": "TM",
            "args": {
                "name": "Run(rnd-xyz$(BUILD_ROOT)/ydb/core/kqp/test-results/foo/testing_out_stuff/chunk1/run)"
            },
        },
        {
            "ph": "B",
            "pid": 1,
            "tid": 7,
            "ts": 40.0,
            "name": "result[TM]",
            "args": {"name": "Result(rnd-xyz)"},
        },
        {
            "ph": "B",
            "pid": 1,
            "tid": 7,
            "ts": 50.0,
            "name": "CC",
            "args": {"name": "Compile(b.cpp)"},
        },
        {"ph": "E", "pid": 1, "tid": 7, "ts": 60.0},
    ]
    activity = parse_evlog_activity(events=events)
    by_kind = {row["kind"]: row for row in activity}
    assert by_kind["test_tm"]["start_us"] == 10.0
    assert by_kind["test_tm"]["end_us"] == 40.0
    assert by_kind["build"]["start_us"] == 50.0
    assert by_kind["build"]["end_us"] == 60.0


def test_evlog_activity_zero_length_close_stays_balanced():
    events = [
        {
            "ph": "B",
            "pid": 1,
            "tid": 1,
            "ts": 10.0,
            "name": "CC",
            "args": {"name": "Compile(zero.cpp)"},
        },
        {"ph": "E", "pid": 1, "tid": 1, "ts": 10.0},
        {
            "ph": "B",
            "pid": 1,
            "tid": 1,
            "ts": 20.0,
            "name": "CC",
            "args": {"name": "Compile(real.cpp)"},
        },
        {"ph": "E", "pid": 1, "tid": 1, "ts": 30.0},
    ]
    activity = parse_evlog_activity(events=events)
    assert activity == [{"kind": "build", "start_us": 20.0, "end_us": 30.0}]


def test_evlog_activity_drains_truncated_open_at_last_ts():
    # B without E/Result — must not leave concurrency stuck after last event.
    events = [
        {
            "ph": "B",
            "pid": 1,
            "tid": 1,
            "ts": 10.0,
            "name": "CC",
            "args": {"name": "Compile(open.cpp)"},
        },
        {
            "ph": "B",
            "pid": 1,
            "tid": 2,
            "ts": 20.0,
            "name": "CC",
            "args": {"name": "Compile(closed.cpp)"},
        },
        {"ph": "E", "pid": 1, "tid": 2, "ts": 40.0},
    ]
    deltas = parse_evlog_activity_deltas(events=events)
    build = sorted(deltas["build"])
    assert build == [(10.0, 1), (20.0, 1), (40.0, -1), (40.0, -1)]
    activity = parse_evlog_activity(events=events)
    assert activity == [
        {"kind": "build", "start_us": 10.0, "end_us": 40.0},
        {"kind": "build", "start_us": 20.0, "end_us": 40.0},
    ]


def test_resources_overlay_keeps_samples_before_and_after_test_chunks():
    enriched_runs = [
        {
            "start_us": 100_000_000.0,
            "end_us": 150_000_000.0,
            "report_suite_start_ts": 1000.0,
        }
    ]
    records = [
        {"ts": 0.0, "cpu_total_pct": 99, "cpu_cores": 8, "ram_used_kb": 999},
        {"ts": 900.0, "cpu_total_pct": 10, "cpu_cores": 8, "ram_used_kb": 1024},
        {"ts": 1100.0, "cpu_total_pct": 20, "cpu_cores": 8, "ram_used_kb": 2048},
    ]
    activity = [{"kind": "compile", "start_us": -50_000_000.0, "end_us": 250_000_000.0}]
    overlay = _build_resources_overlay(
        Path("unused.jsonl"),
        enriched_runs,
        records=records,
        activity=activity,
    )
    assert overlay is not None
    assert overlay["xs_evlog_sec"] == [0.0, 200.0]
    assert overlay["evlog_range_sec"] == [-50.0, 250.0]


def test_active_series_downsampling_preserves_short_peak():
    xs = [float(i) for i in range(100)]
    ys = [0.0] * 100
    ys[51] = 77.0
    _sampled_xs, sampled_ys = downsample_active_series(xs, ys, 10)
    assert max(sampled_ys) == 77.0


def test_dashboard_main_falls_back_to_evlog_when_report_missing():
    # Regression-run_Large can finish without report.json (disk full).
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        report = root / "missing_report.json"
        evlog = root / "ya_evlog.jsonl"
        out_html = root / "dashboard.html"
        # Old worker_threads format — one real chunk run.
        evlog.write_text(
            json.dumps(
                {
                    "namespace": "worker_threads",
                    "event": "node-finished",
                    "thread_name": "worker_1",
                    "value": {
                        "name": "Run($(BUILD_ROOT)/ydb/core/kqp/test-results/foo/testing_out_stuff/chunk1/run)",
                        "time": [10.0, 20.0],
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        script = Path(TEST_METRICS) / "tests_resource_dashboard.py"
        proc = subprocess.run(
            [
                sys.executable,
                str(script),
                "--report",
                str(report),
                "--evlog",
                str(evlog),
                "--out-html",
                str(out_html),
                "--no-csv",
                "--top-n",
                "5",
                "--max-points",
                "100",
                "--repo-root",
                str(Path(__file__).resolve().parents[5]),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr + proc.stdout
        assert out_html.exists()
        data = json.loads((root / "dashboard_data.json").read_text(encoding="utf-8"))
        assert len(data.get("xs_active_exact") or []) >= 2
        stats = json.loads((root / "dashboard_stats.json").read_text(encoding="utf-8"))
        assert stats.get("report_missing_evlog_fallback") is True
        assert int(stats.get("runs_from_evlog") or 0) >= 1


def test_payload_keeps_exact_active_series_past_former_8k_cap():
    # Large (~x4) runs exceed the old silent 8000-point exact→downsampled swap.
    # Click-table counts must stay on the full undownsampled series.
    n_runs = 4500  # 9000 start/end events
    runs = [
        {
            "suite_path": f"s{i % 3}",
            "chunk": i,
            "start_us": float(i * 1_000_000),
            "end_us": float(i * 1_000_000 + 500_000),
            "dur_us": 500_000.0,
            "cpu_sec_report": 0.5,
            "ram_kb_report": 1024.0,
        }
        for i in range(n_runs)
    ]
    payload = build_dashboard_payload(
        suite_filter=None,
        runs=runs,
        stats={},
        top_n=10,
        max_points=1000,
        by_chunk=False,
    )
    exact_xs = payload["xs_active_exact"]
    exact_ys = payload["ys_active_exact"]
    chart_xs = payload["xs_active"]
    assert len(exact_xs) == 2 * n_runs
    assert len(exact_xs) > 8000
    assert len(exact_ys) == len(exact_xs)
    assert len(chart_xs) <= 1000
    assert exact_xs != chart_xs
    # Chart series for test_report is downsampled, but reuse source matches exact.
    report = payload["ya_activity"]["test_report"]
    assert max(report["ys"] or [0]) == max(exact_ys)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for fn in (
        test_report_chunk_key_and_evlog_run_match,
        test_suite_rows_are_not_tests_and_later_chunk_finish_wins,
        test_evlog_activity_covers_compile_link_and_test_jobs,
        test_evlog_activity_chrome_trace_uses_event_name_as_tag,
        test_evlog_activity_closes_tm_on_result_without_e,
        test_evlog_activity_zero_length_close_stays_balanced,
        test_evlog_activity_drains_truncated_open_at_last_ts,
        test_resources_overlay_keeps_samples_before_and_after_test_chunks,
        test_active_series_downsampling_preserves_short_peak,
        test_dashboard_main_falls_back_to_evlog_when_report_missing,
        test_payload_keeps_exact_active_series_past_former_8k_cap,
    ):
        suite.addTest(unittest.FunctionTestCase(fn))
    return suite


if __name__ == "__main__":
    test_report_chunk_key_and_evlog_run_match()
    test_suite_rows_are_not_tests_and_later_chunk_finish_wins()
    test_evlog_activity_covers_compile_link_and_test_jobs()
    test_evlog_activity_chrome_trace_uses_event_name_as_tag()
    test_evlog_activity_closes_tm_on_result_without_e()
    test_evlog_activity_zero_length_close_stays_balanced()
    test_evlog_activity_drains_truncated_open_at_last_ts()
    test_resources_overlay_keeps_samples_before_and_after_test_chunks()
    test_active_series_downsampling_preserves_short_peak()
    test_dashboard_main_falls_back_to_evlog_when_report_missing()
    test_payload_keeps_exact_active_series_past_former_8k_cap()
    print("OK")
