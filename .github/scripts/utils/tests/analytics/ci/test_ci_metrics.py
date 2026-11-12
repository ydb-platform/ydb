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

from collector.spans import read_pending_spans
from github_actions import runner_info
from github_actions.ci_metrics import (
    apply_job_defaults,
    attach_context,
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
    def test_drops_rows_missing_required_fields(self):
        self.assertIsNone(normalize_metric({"name": "job"}))
        almost = {
            "name": "job",
            "source": "ya_phase",
            "run_id": 1,
            "github_job_id": 2,
            "run_attempt": 1,
            "span_id": "span-1",
            "event_ts": "2026-09-21T10:00:00Z",
        }
        for key in ("github_job_id", "run_attempt"):
            row = dict(almost)
            del row[key]
            self.assertIsNone(normalize_metric(row), key)

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
        self.assertNotIn("parent_span_id", json.loads(job_row["labels"]))

        queue_row = next(row for row in rows if row["name"] == "queue")
        self.assertIsNone(queue_row["conclusion"])
        self.assertEqual(json.loads(queue_row["labels"])["parent_span_id"], "job-777")
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


class GithubEnvDefaultsTest(unittest.TestCase):
    def test_job_name_from_github_job_name(self):
        old = {
            "ANALYTICS_JOB_NAME": os.environ.get("ANALYTICS_JOB_NAME"),
            "GITHUB_JOB_NAME": os.environ.get("GITHUB_JOB_NAME"),
            "GITHUB_JOB": os.environ.get("GITHUB_JOB"),
            "GITHUB_RUN_ID": os.environ.get("GITHUB_RUN_ID"),
        }
        try:
            os.environ["ANALYTICS_JOB_NAME"] = "PR-check"
            os.environ["GITHUB_JOB"] = "build_and_test"
            os.environ["GITHUB_JOB_NAME"] = "Build and test relwithdebinfo on main"
            os.environ["GITHUB_RUN_ID"] = "12345"
            defaults = github_env_defaults()
            self.assertEqual(defaults["job_name"], "Build and test relwithdebinfo on main")
            self.assertEqual(defaults["run_id"], 12345)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_falls_back_to_yaml_job_id(self):
        old = {
            "ANALYTICS_JOB_NAME": os.environ.get("ANALYTICS_JOB_NAME"),
            "GITHUB_JOB_NAME": os.environ.get("GITHUB_JOB_NAME"),
            "GITHUB_JOB": os.environ.get("GITHUB_JOB"),
        }
        try:
            os.environ["ANALYTICS_JOB_NAME"] = "PR-check"
            os.environ.pop("GITHUB_JOB_NAME", None)
            os.environ["GITHUB_JOB"] = "build_and_test"
            self.assertEqual(github_env_defaults()["job_name"], "build_and_test")
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
            "ANALYTICS_JOB_NAME",
            "GITHUB_JOB_NAME",
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
                os.environ["ANALYTICS_JOB_NAME"] = "Build and test relwithdebinfo"
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
            "ANALYTICS_JOB_NAME",
            "GITHUB_JOB_NAME",
            "GITHUB_JOB",
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
                os.environ["ANALYTICS_JOB_NAME"] = "build_and_test"
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


class TrackApiTest(unittest.TestCase):
    def test_cli_positional_name_and_duration(self):
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
                        "--label",
                        "cache_mode=none",
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
            self.assertEqual(first["labels"]["cache_mode"], "none")
            self.assertEqual(second["name"], "graph_compare")
            self.assertEqual(second["value"], 2000.0)

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
                            "ya_phase",
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
                self.assertEqual(row["source"], "ya_phase")
                self.assertEqual(row["value"], 10000.0)
                self.assertEqual(row["conclusion"], "success")
                self.assertEqual(row["labels"]["cache_mode"], "dist_cache")
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_track_does_not_end_open_spans(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("ydbd_cached_build", file=path, source="ya_phase")
            track("ydbd_size", {"value": 1, "kind": "gauge"}, file=path, source="ya_phase")
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
            main(["start", "ydbd_cached_build", "--file", self.metrics, "--source", "ya_phase", "--runner"]),
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


class JobDefaultsTest(unittest.TestCase):
    def setUp(self):
        self._saved = {
            key: os.environ.get(key)
            for key in ("CI_YA_ATTEMPT", "CI_BUILD_TARGET")
        }
        for key in self._saved:
            os.environ.pop(key, None)

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_env_labels_and_default_source(self):
        os.environ["CI_YA_ATTEMPT"] = "2"
        os.environ["CI_BUILD_TARGET"] = "ydb"
        props, fields = {}, {}
        apply_job_defaults("prepare_ya_make", props, fields, command="start")
        self.assertEqual(props["ya_attempt"], "2")
        self.assertEqual(props["build_target"], "ydb")
        self.assertNotIn("cache_mode", props)
        self.assertEqual(fields["source"], "ya_phase")
        self.assertNotIn("runner", fields)

    def test_ya_make_try_gets_runner_and_usage(self):
        start_fields: dict = {}
        apply_job_defaults("ya_make_try_1", {}, start_fields, command="start")
        self.assertTrue(start_fields["runner"])
        self.assertEqual(start_fields["source"], "ya_phase")
        end_fields: dict = {}
        apply_job_defaults("ya_make_try_1", {}, end_fields, command="end")
        self.assertTrue(end_fields["usage"])

    def test_rc_sets_conclusion_and_error(self):
        props, fields = {}, {"rc": "7"}
        apply_job_defaults("postprocess_try", props, fields, command="end")
        self.assertEqual(fields["conclusion"], "failure")
        self.assertEqual(props["error"], "postprocess_try rc=7")
        self.assertNotIn("rc", fields)
        ok, zero = {}, {"rc": "0"}
        apply_job_defaults("postprocess_try", ok, zero, command="end")
        self.assertEqual(zero["conclusion"], "success")
        self.assertNotIn("error", ok)

    def test_start_reads_env_into_the_pending_span(self):
        os.environ["CI_YA_ATTEMPT"] = "3"
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("prepare_ya_make", file=path)
            pending = read_pending_spans(path)
            self.assertEqual(pending[0]["source"], "ya_phase")
            self.assertEqual(pending[0]["labels"]["ya_attempt"], "3")
            self.assertNotIn("cache_mode", pending[0]["labels"])

    def test_enrich_matches_props_ya_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("ya_make_try_1", {"ya_attempt": "1"}, file=path)
            end("ya_make_try_1", file=path, conclusion="success")
            start("ya_make_try_1", {"ya_attempt": "2"}, file=path)
            end("ya_make_try_1", file=path, conclusion="success")
            enrich("ya_make_try_1", {"ya_attempt": "1", "report_url": "try1"}, file=path)
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            by_attempt = {row["labels"]["ya_attempt"]: row for row in rows}
            self.assertEqual(by_attempt["1"]["labels"]["report_url"], "try1")
            self.assertNotIn("report_url", by_attempt["2"]["labels"])

    def test_cli_end_rc(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            self.assertEqual(main(["start", "graph_compare", "--file", path]), 0)
            self.assertEqual(main(["end", "graph_compare", "--file", path, "--rc", "4"]), 0)
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["conclusion"], "failure")
            self.assertEqual(row["labels"]["error"], "graph_compare rc=4")


if __name__ == "__main__":
    unittest.main()

