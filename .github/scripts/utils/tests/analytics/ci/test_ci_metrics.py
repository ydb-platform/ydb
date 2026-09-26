#!/usr/bin/env python3
"""Unit tests for the generic CI metrics client. No YDB required."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from collector.buffer import load_unsent_lines, write_send_offset
from collector.spans import read_pending_spans
from github_actions import runner_info
from github_actions.ci_metrics import (
    DEFAULT_TABLE_PATH,
    PRIMARY_KEYS,
    attach_context,
    build_create_table_sql,
    github_env_defaults,
    main,
    normalize_metric,
    rows_from_jsonl,
    start,
    end,
    enrich,
    track,
)
from github_actions.export_github_job_metrics import guess_build_preset, metrics_from_workflow_run
from github_actions.runner_info import INVENTORY_LABEL, USAGE_LABEL


class GuessBuildPresetTest(unittest.TestCase):
    def test_pr_check_job_name(self):
        self.assertEqual(guess_build_preset("Build and test relwithdebinfo"), "relwithdebinfo")
        self.assertEqual(guess_build_preset("Build and test release-asan"), "release-asan")

    def test_postcommit_job_name(self):
        self.assertEqual(
            guess_build_preset("Postcommit · Build and test relwithdebinfo"),
            "relwithdebinfo",
        )

    def test_unknown(self):
        self.assertIsNone(guess_build_preset("check-running-allowed"))
        self.assertIsNone(guess_build_preset(""))


class NormalizeMetricTest(unittest.TestCase):
    def test_requires_name_and_run_id(self):
        self.assertIsNone(normalize_metric({"name": "job"}))
        self.assertIsNone(
            normalize_metric(
                {
                    "name": "job",
                    "event_ts": "2026-09-21T10:00:00Z",
                }
            )
        )

    def test_requires_github_job_id(self):
        self.assertIsNone(
            normalize_metric(
                {
                    "name": "job",
                    "source": "ya_phase",
                    "run_id": 1,
                    "run_attempt": 1,
                    "span_id": "span-1",
                    "event_ts": "2026-09-21T10:00:00Z",
                }
            )
        )

    def test_requires_run_attempt(self):
        self.assertIsNone(
            normalize_metric(
                {
                    "name": "job",
                    "source": "ya_phase",
                    "run_id": 1,
                    "github_job_id": 2,
                    "span_id": "span-1",
                    "event_ts": "2026-09-21T10:00:00Z",
                }
            )
        )

    def test_duration_from_epoch_and_labels(self):
        row = normalize_metric(
            {
                "name": "ya_make_try_1",
                "source": "ya_phase",
                "started_at": 1726900000,
                "run_id": 123,
                "duration_ms": 45000,
                "conclusion": "success",
                "labels": {"cache_mode": "dist_cache", "ya_attempt": 1},
                "github_job_id": 555,
                "run_attempt": 1,
                "span_id": "span-ya",
            }
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["run_id"], 123)
        self.assertEqual(row["name"], "ya_make_try_1")
        self.assertEqual(row["kind"], "duration")
        self.assertEqual(row["value"], 45000.0)
        self.assertEqual(row["unit"], "ms")
        self.assertEqual(row["date"], datetime.fromtimestamp(1726900000, tz=timezone.utc).date())
        self.assertEqual(row["github_job_id"], 555)
        labels = json.loads(row["labels"])
        self.assertEqual(labels["cache_mode"], "dist_cache")
        self.assertEqual(labels["ya_attempt"], 1)
        self.assertEqual(labels["parent_span_id"], "job-555")
        self.assertEqual(row["source"], "ya_phase")

    def test_finished_epoch_needs_source_and_job_id(self):
        self.assertIsNone(
            normalize_metric(
                {
                    "name": "s3_sync_try",
                    "run_id": 7,
                    "started_at": "1000",
                    "finished_epoch": "1010",
                }
            )
        )
        row = normalize_metric(
            {
                "name": "s3_sync_try",
                "source": "ya_phase",
                "run_id": 7,
                "github_job_id": 9,
                "run_attempt": 1,
                "span_id": "span-s3",
                "started_at": "1000",
                "finished_epoch": "1010",
            }
        )
        self.assertEqual(row["value"], 10000.0)
        self.assertEqual(row["source"], "ya_phase")
        self.assertEqual(row["github_job_id"], 9)

    def test_gauge(self):
        row = normalize_metric(
            {
                "name": "ydbd_size",
                "kind": "gauge",
                "value": 1048576,
                "unit": "bytes",
                "source": "clean_build",
                "run_id": 9,
                "github_job_id": 3,
                "run_attempt": 1,
                "span_id": "span-size",
                "event_ts": "2026-09-21T03:00:00Z",
            }
        )
        self.assertEqual(row["kind"], "gauge")
        self.assertEqual(row["value"], 1048576.0)
        self.assertEqual(row["unit"], "bytes")
        self.assertEqual(row["source"], "clean_build")


class JsonlRowsTest(unittest.TestCase):
    def test_skips_bad_lines_and_applies_defaults(self):
        payload = "\n".join(
            [
                "not-json",
                json.dumps(
                    {
                        "name": "graph_compare",
                        "source": "ya_phase",
                        "event_ts": "2026-09-21T10:00:00Z",
                        "value": 1200,
                    }
                ),
                "",
            ]
        )
        rows = rows_from_jsonl(
            io.StringIO(payload),
            defaults={
                "run_id": 99,
                "github_job_id": 1,
                "run_attempt": 1,
                "span_id": "span-jsonl",
                "build_preset": "relwithdebinfo",
            },
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], 99)
        self.assertEqual(rows[0]["build_preset"], "relwithdebinfo")
        self.assertEqual(rows[0]["name"], "graph_compare")
        self.assertEqual(rows[0]["source"], "ya_phase")


class WorkflowRunMetricsTest(unittest.TestCase):
    def test_job_queue_and_steps(self):
        run = {
            "id": 555,
            "name": "PR-check",
            "event": "pull_request_target",
            "head_sha": "abc123",
            "head_branch": "feature",
            "run_attempt": 1,
            "html_url": "https://github.com/ydb-platform/ydb/actions/runs/555",
            "created_at": "2026-09-21T10:00:00Z",
            "pull_requests": [{"number": 42, "base": {"ref": "main"}}],
        }
        jobs = [
            {
                "id": 777,
                "name": "Build and test relwithdebinfo",
                "created_at": "2026-09-21T10:04:00Z",
                "started_at": "2026-09-21T10:05:00Z",
                "completed_at": "2026-09-21T11:05:00Z",
                "conclusion": "success",
                "steps": [
                    {
                        "name": "Checkout",
                        "started_at": "2026-09-21T10:05:10Z",
                        "completed_at": "2026-09-21T10:06:10Z",
                        "conclusion": "success",
                    },
                    {
                        "name": "ya build and test",
                        "started_at": "2026-09-21T10:07:00Z",
                        "completed_at": "2026-09-21T11:00:00Z",
                        "conclusion": "success",
                    },
                    {"name": "skipped step", "conclusion": "skipped"},
                ],
            }
        ]
        rows = metrics_from_workflow_run(run, jobs)
        keys = {(row["source"], row["name"]) for row in rows}
        self.assertIn(("github_job", "job"), keys)
        self.assertIn(("github_job", "queue"), keys)
        self.assertIn(("github_step", "Checkout"), keys)
        job_sources = {row["source"] for row in rows if row["name"] == "job"}
        self.assertEqual(job_sources, {"github_job"})
        self.assertIn(("github_step", "ya build and test"), keys)
        self.assertNotIn(("github_step", "skipped step"), keys)

        job_row = next(row for row in rows if row["name"] == "job")
        self.assertEqual(job_row["run_id"], 555)
        self.assertEqual(job_row["github_job_id"], 777)
        self.assertEqual(job_row["pr_number"], 42)
        self.assertEqual(job_row["branch"], "main")
        self.assertEqual(job_row["build_preset"], "relwithdebinfo")
        self.assertEqual(job_row["kind"], "duration")
        self.assertEqual(job_row["value"], 60 * 60 * 1000)
        self.assertEqual(json.loads(job_row["labels"])["queued_ms"], 60 * 1000)
        self.assertEqual(json.loads(job_row["labels"])["parent_span_id"], "job-777")

        queue_row = next(row for row in rows if row["name"] == "queue")
        self.assertEqual(queue_row["value"], 60 * 1000)
        self.assertEqual(queue_row["event_ts"], datetime(2026, 9, 21, 10, 4, tzinfo=timezone.utc))

    def test_skips_job_without_id(self):
        run = {"id": 1, "event": "push", "run_attempt": 1, "created_at": "2026-09-21T10:00:00Z"}
        jobs = [
            {
                "name": "Build and test relwithdebinfo",
                "started_at": "2026-09-21T10:01:00Z",
                "completed_at": "2026-09-21T10:02:00Z",
                "conclusion": "success",
            }
        ]
        self.assertEqual(metrics_from_workflow_run(run, jobs), [])

    def test_queue_falls_back_to_run_created_when_job_created_missing(self):
        run = {
            "id": 1,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
            "run_attempt": 1,
            "created_at": "2026-09-21T10:00:00Z",
        }
        jobs = [
            {
                "id": 2,
                "name": "Build and test relwithdebinfo",
                "started_at": "2026-09-21T10:03:00Z",
                "completed_at": "2026-09-21T10:04:00Z",
                "conclusion": "success",
            }
        ]
        rows = metrics_from_workflow_run(run, jobs)
        queue_row = next(row for row in rows if row["name"] == "queue")
        self.assertEqual(queue_row["value"], 3 * 60 * 1000)

    def test_pr_without_pulls_keeps_head_branch(self):
        run = {
            "id": 2,
            "event": "pull_request_target",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "feature",
            "run_attempt": 1,
            "created_at": "2026-09-21T10:00:00Z",
        }
        jobs = [
            {
                "id": 3,
                "name": "Build and test relwithdebinfo",
                "created_at": "2026-09-21T10:00:00Z",
                "started_at": "2026-09-21T10:01:00Z",
                "completed_at": "2026-09-21T10:02:00Z",
                "conclusion": "success",
            }
        ]
        rows = metrics_from_workflow_run(run, jobs)
        self.assertTrue(all(row.get("branch") == "feature" for row in rows))

    def test_push_ignores_associated_pull_requests(self):
        run = {
            "id": 1,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
            "run_attempt": 1,
            "created_at": "2026-09-21T10:00:00Z",
            "pull_requests": [{"number": 10, "base": {"ref": "main"}}],
        }
        jobs = [
            {
                "id": 2,
                "name": "Postcommit · Build and test relwithdebinfo",
                "created_at": "2026-09-21T10:00:00Z",
                "started_at": "2026-09-21T10:03:00Z",
                "completed_at": "2026-09-21T10:04:00Z",
                "conclusion": "success",
            }
        ]
        rows = metrics_from_workflow_run(run, jobs)
        self.assertTrue(all(row.get("pr_number") is None for row in rows))
        self.assertTrue(all(row.get("branch") == "main" for row in rows))


class SchemaTest(unittest.TestCase):
    def test_create_sql_has_pk_and_ttl(self):
        sql = build_create_table_sql(DEFAULT_TABLE_PATH)
        self.assertIn(DEFAULT_TABLE_PATH, sql)
        self.assertIn("STORE = COLUMN", sql)
        self.assertIn('TTL = Interval("PT525600M")', sql)
        self.assertIn("ON event_ts", sql)
        self.assertIn("PRIMARY KEY (`event_ts`", sql)
        for key in PRIMARY_KEYS:
            self.assertIn(f"`{key}`", sql)
        self.assertIn("`source` Utf8 NOT NULL", sql)


class GithubEnvDefaultsTest(unittest.TestCase):
    def test_prefers_ci_job_title(self):
        old = {
            "CI_JOB_TITLE": os.environ.get("CI_JOB_TITLE"),
            "GITHUB_RUN_ID": os.environ.get("GITHUB_RUN_ID"),
        }
        try:
            os.environ["CI_JOB_TITLE"] = "Build and test relwithdebinfo"
            os.environ["GITHUB_RUN_ID"] = "12345"
            defaults = github_env_defaults()
            self.assertEqual(defaults["job_name"], "Build and test relwithdebinfo")
            self.assertEqual(defaults["run_id"], 12345)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_reads_pull_request_from_event_path(self):
        old = {key: os.environ.get(key) for key in (
            "GITHUB_EVENT_PATH",
            "PR_NUMBER",
            "ORIGINAL_HEAD",
            "GITHUB_SHA",
            "BRANCH_NAME",
            "GITHUB_BASE_REF",
            "GITHUB_REF_NAME",
            "BUILD_PRESET",
            "CI_JOB_TITLE",
            "GITHUB_JOB",
        )}
        try:
            for key in old:
                os.environ.pop(key, None)
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "event.json")
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(
                        {
                            "number": 53660,
                            "pull_request": {
                                "number": 53660,
                                "head": {"sha": "abc123def"},
                                "base": {"ref": "main"},
                            },
                        },
                        handle,
                    )
                os.environ["GITHUB_EVENT_PATH"] = path
                os.environ["CI_JOB_TITLE"] = "Build and test relwithdebinfo"
                os.environ["BUILD_PRESET"] = "relwithdebinfo"
                defaults = github_env_defaults()
            self.assertEqual(defaults["pr_number"], 53660)
            self.assertEqual(defaults["commit"], "abc123def")
            self.assertEqual(defaults["branch"], "main")
            self.assertEqual(defaults["build_preset"], "relwithdebinfo")
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_event_path_fills_columns_not_labels(self):
        old = {key: os.environ.get(key) for key in (
            "GITHUB_EVENT_PATH",
            "GITHUB_EVENT_NAME",
            "GITHUB_WORKFLOW",
            "GITHUB_RUN_ID",
            "GITHUB_SHA",
            "PR_NUMBER",
            "ORIGINAL_HEAD",
            "BRANCH_NAME",
            "CI_JOB_TITLE",
            "GITHUB_TOKEN",
            "GITHUB_REPOSITORY",
            "GITHUB_NUMERIC_JOB_ID",
        )}
        try:
            for key in old:
                os.environ.pop(key, None)
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "event.json")
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(
                        {
                            "number": 53660,
                            "pull_request": {
                                "number": 53660,
                                "head": {"sha": "abc123def"},
                                "base": {"ref": "main"},
                            },
                        },
                        handle,
                    )
                os.environ["GITHUB_EVENT_PATH"] = path
                os.environ["GITHUB_EVENT_NAME"] = "pull_request_target"
                os.environ["GITHUB_WORKFLOW"] = "PR-check"
                os.environ["GITHUB_RUN_ID"] = "99"
                os.environ["CI_JOB_TITLE"] = "build_and_test"
                record = attach_context({"name": "ya_make_try_1", "source": "ya_phase"})
            self.assertEqual(record["pr_number"], 53660)
            self.assertEqual(record["event_name"], "pull_request_target")
            self.assertEqual(record["commit"], "abc123def")
            self.assertEqual(record["workflow"], "PR-check")
            self.assertNotIn("github_job_id", record)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_uses_numeric_job_id_from_env(self):
        old = os.environ.get("GITHUB_NUMERIC_JOB_ID")
        try:
            os.environ["GITHUB_NUMERIC_JOB_ID"] = "778899"
            self.assertEqual(github_env_defaults()["github_job_id"], 778899)
        finally:
            if old is None:
                os.environ.pop("GITHUB_NUMERIC_JOB_ID", None)
            else:
                os.environ["GITHUB_NUMERIC_JOB_ID"] = old


class RecordApiTest(unittest.TestCase):
    def test_track_and_cli_append_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            track(
                "graph_compare",
                file=path,
                source="ya_phase",
                started_epoch="1000",
                finished_epoch="1002",
                conclusion="success",
                labels={"cache_mode": "dist_cache"},
            )
            self.assertEqual(
                main(
                    [
                        "track",
                        "--file",
                        path,
                        "--name",
                        "ydbd_size",
                        "--kind",
                        "gauge",
                        "--unit",
                        "bytes",
                        "--source",
                        "clean_build",
                        "--value",
                        "42",
                        "--label",
                        "cache_mode=none",
                    ]
                ),
                0,
            )
            with open(path, encoding="utf-8") as handle:
                first, second = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual(first["name"], "graph_compare")
            self.assertEqual(first["value"], 2000.0)
            self.assertEqual(second["name"], "ydbd_size")
            self.assertEqual(second["kind"], "gauge")
            self.assertEqual(second["value"], 42.0)


class TrackApiTest(unittest.TestCase):
    def test_cli_oneliner_positional_and_duration_sec(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            self.assertEqual(
                main(
                    [
                        "track",
                        "wait_for_lock",
                        "--file",
                        path,
                        "--source",
                        "other_wf",
                        "--duration-ms",
                        "2500",
                        "--conclusion",
                        "success",
                    ]
                ),
                0,
            )
            self.assertEqual(
                main(
                    [
                        "track",
                        "--name",
                        "graph_compare",
                        "--file",
                        path,
                        "--source",
                        "ya_phase",
                        "--started-epoch",
                        "1000",
                        "--finished-epoch",
                        "1002",
                    ]
                ),
                0,
            )
            with open(path, encoding="utf-8") as handle:
                first, second = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual(first["name"], "wait_for_lock")
            self.assertEqual(first["kind"], "duration")
            self.assertEqual(first["value"], 2500.0)
            self.assertEqual(first["source"], "other_wf")
            self.assertEqual(second["name"], "graph_compare")
            self.assertEqual(second["value"], 2000.0)

    def test_cli_track_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            self.assertEqual(
                main(
                    [
                        "track",
                        "--file",
                        path,
                        "--name",
                        "ydbd_size",
                        "--kind",
                        "gauge",
                        "--value",
                        "42",
                        "--unit",
                        "bytes",
                        "--source",
                        "clean_build",
                        "--label",
                        "cache_mode=none",
                    ]
                ),
                0,
            )
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], "ydbd_size")
            self.assertEqual(row["kind"], "gauge")
            self.assertEqual(row["value"], 42.0)
            self.assertEqual(row["labels"]["cache_mode"], "none")

    def test_track_does_not_flush(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import github_actions.ci_metrics as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "ci_metrics.jsonl")
                track("a", {"value": 1, "source": "test"}, file=path)
                track("b", {"value": 2, "source": "test"}, file=path)
                self.assertEqual(sends, [])
                with open(path, encoding="utf-8") as handle:
                    self.assertEqual(len([line for line in handle if line.strip()]), 2)
        finally:
            client.flush_file = original

    def test_unsent_offset(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            first = json.dumps({"name": "one"}) + "\n"
            second = json.dumps({"name": "two"}) + "\n"
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(first + second)
            write_send_offset(path, len(first.encode("utf-8")))
            lines, offset = load_unsent_lines(path)
            self.assertEqual(lines, [json.dumps({"name": "two"})])
            self.assertEqual(offset, len((first + second).encode("utf-8")))

    def test_track_sets_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            track("graph_compare", {"value": 5, "conclusion": "success"}, file=path, source="ya_phase")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], "graph_compare")
            self.assertEqual(row["source"], "ya_phase")
            self.assertEqual(row["value"], 5.0)

    def test_enrich_via_functions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("ya_make", file=path, source="ya_phase", started_epoch="1000")
            self.assertEqual(end("ya_make", file=path, conclusion="success", finished_epoch="1003"), 1)
            self.assertEqual(enrich("ya_make", {"report_url": "https://s3.example/ya"}, file=path), 1)
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["value"], 3000.0)
            self.assertEqual(row["labels"]["report_url"], "https://s3.example/ya")

    def test_start_send_computes_duration(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import github_actions.ci_metrics as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "ci_metrics.jsonl")
                self.assertEqual(
                    main(
                        [
                            "start",
                            "ydbd_cached_build",
                            "--file",
                            path,
                            "--source",
                            "nightly_build",
                            "--started-epoch",
                            "1000",
                            "--label",
                            "cache_mode=dist_cache",
                        ]
                    ),
                    0,
                )
                self.assertTrue(read_pending_spans(path))
                self.assertFalse(os.path.exists(path) and os.path.getsize(path))
                self.assertEqual(
                    main(
                        [
                            "send",
                            "--file",
                            path,
                            "--conclusion",
                            "success",
                            "--finished-epoch",
                            "1010",
                        ]
                    ),
                    0,
                )
                self.assertEqual(read_pending_spans(path), [])
                with open(path, encoding="utf-8") as handle:
                    row = json.loads(handle.readline())
                self.assertEqual(row["name"], "ydbd_cached_build")
                self.assertEqual(row["kind"], "duration")
                self.assertEqual(row["source"], "nightly_build")
                self.assertEqual(row["value"], 10000.0)
                self.assertEqual(row["conclusion"], "success")
                self.assertEqual(row["labels"]["cache_mode"], "dist_cache")
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_track_does_not_end_open_spans(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("ydbd_cached_build", file=path, source="nightly_build")
            track("ydbd_size", {"value": 1, "kind": "gauge"}, file=path, source="nightly_build")
            pending = read_pending_spans(path)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["name"], "ydbd_cached_build")
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual([row["name"] for row in rows], ["ydbd_size"])


class RunnerFlagsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.metrics = os.path.join(self.tmp.name, "ci_metrics.jsonl")
        self.cache = os.path.join(self.tmp.name, "runner.json")
        self.saved = {key: os.environ.get(key) for key in ("CI_RUNNER_INFO_FILE", "RUNNER_TEMP")}
        os.environ["CI_RUNNER_INFO_FILE"] = self.cache
        os.environ.pop("RUNNER_TEMP", None)
        self.inv_calls = 0
        self.use_calls = 0
        self.orig_inv = runner_info.collect_inventory
        self.orig_use = runner_info.collect_usage
        runner_info.collect_inventory = self._inventory
        runner_info.collect_usage = self._usage

    def tearDown(self):
        runner_info.collect_inventory = self.orig_inv
        runner_info.collect_usage = self.orig_use
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _inventory(self):
        self.inv_calls += 1
        return {
            "cpu_count": 8,
            "mem_total_bytes": 16 * 1024**3,
            "disk_total_bytes": 100 * 1024**3,
        }

    def _usage(self):
        self.use_calls += 1
        return {
            "cpu_pct": 10.0 * self.use_calls,
            "loadavg_1": 0.5,
            "mem_used_bytes": 1024 * self.use_calls,
            "disk_used_bytes": 2048 * self.use_calls,
        }

    def test_no_flags_skips_proc(self):
        start("step", file=self.metrics, source="wf")
        pending = read_pending_spans(self.metrics)
        labels = pending[0].get("labels") or {}
        self.assertNotIn(INVENTORY_LABEL, labels)
        self.assertNotIn(USAGE_LABEL, labels)
        self.assertEqual(self.inv_calls, 0)
        self.assertEqual(self.use_calls, 0)

    def test_runner_collects_once_and_reuses(self):
        self.assertEqual(
            main(["start", "ydbd_cached_build", "--file", self.metrics, "--source", "nightly_build", "--runner"]),
            0,
        )
        self.assertEqual(self.inv_calls, 1)
        first = read_pending_spans(self.metrics)[0]["labels"][INVENTORY_LABEL]
        self.assertEqual(first["cpu_count"], 8)
        self.assertEqual(first["disk_total_bytes"], 100 * 1024**3)
        self.assertTrue(os.path.isfile(self.cache))
        self.assertEqual(
            main(["track", "ydbd_size", "--file", self.metrics, "--kind", "gauge", "--value", "1", "--runner"]),
            0,
        )
        self.assertEqual(self.inv_calls, 1)
        with open(self.metrics, encoding="utf-8") as handle:
            row = json.loads(handle.readline())
        self.assertEqual(row["labels"][INVENTORY_LABEL]["mem_total_bytes"], 16 * 1024**3)
        self.assertNotIn("runner", row["labels"])

    def test_usage_is_per_snapshot(self):
        self.assertEqual(
            main(["track", "ydbd_size", "--file", self.metrics, "--kind", "gauge", "--value", "1", "--usage"]),
            0,
        )
        self.assertEqual(
            main(["track", "ydbd_size", "--file", self.metrics, "--kind", "gauge", "--value", "2", "--usage"]),
            0,
        )
        self.assertEqual(self.use_calls, 2)
        self.assertEqual(self.inv_calls, 0)
        with open(self.metrics, encoding="utf-8") as handle:
            rows = [json.loads(line) for line in handle if line.strip()]
        self.assertEqual(rows[0]["labels"][USAGE_LABEL]["cpu_pct"], 10.0)
        self.assertEqual(rows[1]["labels"][USAGE_LABEL]["cpu_pct"], 20.0)
        self.assertFalse(os.path.exists(self.cache))

    def test_start_runner_end_usage(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import github_actions.ci_metrics as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            start("ydbd_clean_build", file=self.metrics, source="clean_build", runner=True, started_epoch="1000")
            self.assertEqual(self.inv_calls, 1)
            self.assertEqual(
                main(
                    [
                        "send",
                        "--file",
                        self.metrics,
                        "--conclusion",
                        "success",
                        "--finished-epoch",
                        "1010",
                        "--usage",
                    ]
                ),
                0,
            )
            with open(self.metrics, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            labels = row["labels"]
            self.assertEqual(labels[INVENTORY_LABEL]["cpu_count"], 8)
            self.assertEqual(labels[USAGE_LABEL]["mem_used_bytes"], 1024)
            self.assertNotIn("usage", labels)
            self.assertEqual(self.use_calls, 1)
            self.assertEqual(sends, [self.metrics])
        finally:
            client.flush_file = original

    def test_enrich_cli_adds_url_keeps_duration(self):
        start("dashboard", file=self.metrics, source="ya_phase", started_epoch="1000")
        end("dashboard", file=self.metrics, conclusion="success", finished_epoch="1004")
        self.assertEqual(
            main(
                [
                    "enrich",
                    "dashboard",
                    "--file",
                    self.metrics,
                    "--label",
                    "report_url=https://s3.example/dashboard.html",
                    "--error",
                    "should-not-change-conclusion",
                ]
            ),
            0,
        )
        with open(self.metrics, encoding="utf-8") as handle:
            row = json.loads(handle.readline())
        self.assertEqual(row["value"], 4000.0)
        self.assertEqual(row["conclusion"], "success")
        self.assertEqual(row["labels"]["report_url"], "https://s3.example/dashboard.html")
        self.assertEqual(row["labels"]["error"], "should-not-change-conclusion")


class FlushBehaviorTest(unittest.TestCase):
    def test_mixed_batch_writes_skipped_and_uploads_valid(self):
        created = []

        class Wrapper:
            def __init__(self):
                created.append(self)
                self.created_tables = []

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def check_credentials(self):
                return True

            def get_table_path(self, key):
                raise KeyError(key)

            def create_table(self, path, sql):
                self.created_tables.append(path)

        import github_actions.ci_metrics as client

        original = client.collector_upsert_metrics
        saved = os.environ.get("ANALYTICS_YDB_CREDENTIALS")
        os.environ["ANALYTICS_YDB_CREDENTIALS"] = "1"
        uploaded = []

        def fake_upsert(wrapper, rows, **kwargs):
            uploaded.append((rows, kwargs.get("ensure_table")))
            return len(rows)

        client.collector_upsert_metrics = fake_upsert
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "ci_metrics.jsonl")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(json.dumps({"name": "bad"}) + "\n")
                    handle.write(
                        json.dumps(
                            {
                                "name": "ok",
                                "source": "ya_phase",
                                "run_id": 1,
                                "github_job_id": 2,
                                "run_attempt": 1,
                                "span_id": "span-ok",
                                "event_ts": "2026-09-21T10:00:00Z",
                                "value": 1,
                            }
                        )
                        + "\n"
                    )
                count = client.flush_file(path, ydb_wrapper_factory=lambda: Wrapper)
                self.assertEqual(count, 1)
                self.assertEqual(uploaded[0][1], False)
                skipped = Path(path + ".skipped").read_text(encoding="utf-8")
                self.assertIn("no event_ts", skipped)
        finally:
            client.collector_upsert_metrics = original
            if saved is None:
                os.environ.pop("ANALYTICS_YDB_CREDENTIALS", None)
            else:
                os.environ["ANALYTICS_YDB_CREDENTIALS"] = saved


if __name__ == "__main__":
    unittest.main()

