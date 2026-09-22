#!/usr/bin/env python3
"""Unit tests for the generic CI metrics client. No YDB required."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

import runner_info
from runner_info import INVENTORY_LABEL, USAGE_LABEL

from core import (
    append_record,
    build_track_record,
    load_unsent_lines,
    parse_datetime,
    parse_labels,
    read_pending_spans,
    write_send_offset,
)
from ci_metrics import (
    BUILD_INFO_NAME,
    DEFAULT_TABLE_PATH,
    PRIMARY_KEYS,
    Analytics,
    attach_context,
    build_create_table_sql,
    github_context_labels,
    github_env_defaults,
    guess_build_preset,
    main,
    metrics_from_workflow_run,
    normalize_metric,
    rows_from_jsonl,
    start,
    end,
    timed,
    track,
)


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
            }
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["run_id"], 123)
        self.assertEqual(row["name"], "ya_make_try_1")
        self.assertEqual(row["kind"], "duration")
        self.assertEqual(row["value"], 45000.0)
        self.assertEqual(row["unit"], "ms")
        self.assertEqual(row["date"], datetime.fromtimestamp(1726900000, tz=timezone.utc).date())
        self.assertEqual(row["github_job_id"], 0)
        labels = json.loads(row["labels"])
        self.assertEqual(labels["cache_mode"], "dist_cache")
        self.assertEqual(labels["ya_attempt"], 1)
        self.assertEqual(row["source"], "ya_phase")

    def test_finished_epoch_and_unknown_source(self):
        row = normalize_metric(
            {
                "name": "s3_sync_try",
                "run_id": 7,
                "started_at": "1000",
                "finished_epoch": "1010",
            }
        )
        self.assertEqual(row["value"], 10000.0)
        self.assertEqual(row["source"], "unknown")

    def test_build_info_snapshot(self):
        row = normalize_metric(
            {
                "name": "build_info",
                "kind": "info",
                "source": "nightly_build",
                "run_id": 11,
                "event_ts": "2026-09-21T03:00:00Z",
                "labels": {"payload": {"nodes": [{"name": "a.cpp", "duration_ms": 10}]}},
            }
        )
        self.assertEqual(row["kind"], "info")
        self.assertIsNone(row["value"])
        self.assertEqual(json.loads(row["labels"])["payload"]["nodes"][0]["name"], "a.cpp")

    def test_gauge(self):
        row = normalize_metric(
            {
                "name": "ydbd_size",
                "kind": "gauge",
                "value": 1048576,
                "unit": "bytes",
                "source": "clean_build",
                "run_id": 9,
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
        rows = rows_from_jsonl(io.StringIO(payload), defaults={"run_id": 99, "build_preset": "relwithdebinfo"})
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

        queue_row = next(row for row in rows if row["name"] == "queue")
        self.assertEqual(queue_row["value"], 60 * 1000)
        self.assertEqual(queue_row["event_ts"], datetime(2026, 9, 21, 10, 4, tzinfo=timezone.utc))

    def test_queue_falls_back_to_run_created_when_job_created_missing(self):
        run = {
            "id": 1,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
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
        self.assertIn("TTL = Interval", sql)
        self.assertIn("ON event_ts", sql)
        self.assertIn("PRIMARY KEY (`event_ts`", sql)
        for key in PRIMARY_KEYS:
            self.assertIn(f"`{key}`", sql)
        self.assertIn("`source` Utf8 NOT NULL", sql)


class GithubEnvDefaultsTest(unittest.TestCase):
    def test_prefers_ci_job_title(self):
        old = {
            "CI_JOB_TITLE": os.environ.get("CI_JOB_TITLE"),
            "ANALYTICS_JOB_NAME": os.environ.get("ANALYTICS_JOB_NAME"),
            "GITHUB_RUN_ID": os.environ.get("GITHUB_RUN_ID"),
        }
        try:
            os.environ["CI_JOB_TITLE"] = "Build and test relwithdebinfo"
            os.environ["ANALYTICS_JOB_NAME"] = "PR-check"
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
            "GITHUB_PR_NUMBER",
            "ORIGINAL_HEAD",
            "GITHUB_SHA",
            "BRANCH_NAME",
            "GITHUB_BASE_REF",
            "GITHUB_REF_NAME",
            "BUILD_PRESET",
            "CI_JOB_TITLE",
            "ANALYTICS_JOB_NAME",
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

    def test_attaches_github_event_entities(self):
        old = {key: os.environ.get(key) for key in (
            "GITHUB_EVENT_PATH",
            "GITHUB_EVENT_NAME",
            "GITHUB_WORKFLOW",
            "GITHUB_RUN_ID",
            "GITHUB_RUN_ATTEMPT",
            "GITHUB_SHA",
            "GITHUB_REF",
            "GITHUB_REF_NAME",
            "GITHUB_BASE_REF",
            "GITHUB_HEAD_REF",
            "GITHUB_JOB",
            "GITHUB_REPOSITORY",
            "GITHUB_ACTOR",
            "PR_NUMBER",
            "GITHUB_PR_NUMBER",
            "ORIGINAL_HEAD",
            "BRANCH_NAME",
            "CI_JOB_TITLE",
        )}
        try:
            for key in old:
                os.environ.pop(key, None)
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "event.json")
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(
                        {
                            "action": "synchronize",
                            "number": 53660,
                            "pull_request": {
                                "number": 53660,
                                "html_url": "https://github.com/ydb-platform/ydb/pull/53660",
                                "state": "open",
                                "draft": False,
                                "merged": False,
                                "user": {"login": "naspirato"},
                                "head": {
                                    "ref": "cursor/ci-pr-check-observability-7839",
                                    "sha": "abc123def",
                                    "repo": {"full_name": "naspirato/ydb"},
                                },
                                "base": {
                                    "ref": "main",
                                    "sha": "def456",
                                    "repo": {"full_name": "ydb-platform/ydb"},
                                },
                                "labels": [{"name": "ci"}],
                                "body": "should-not-be-copied",
                            },
                            "repository": {"full_name": "ydb-platform/ydb", "default_branch": "main"},
                            "sender": {"login": "naspirato"},
                        },
                        handle,
                    )
                os.environ["GITHUB_EVENT_PATH"] = path
                os.environ["GITHUB_EVENT_NAME"] = "pull_request_target"
                os.environ["GITHUB_WORKFLOW"] = "PR-check"
                os.environ["GITHUB_RUN_ID"] = "99"
                os.environ["GITHUB_RUN_ATTEMPT"] = "2"
                os.environ["GITHUB_SHA"] = "mergecommit"
                os.environ["GITHUB_REF"] = "refs/heads/main"
                os.environ["GITHUB_JOB"] = "build_and_test"
                os.environ["GITHUB_REPOSITORY"] = "ydb-platform/ydb"
                os.environ["GITHUB_ACTOR"] = "naspirato"
                labels = github_context_labels()
                record = attach_context({"name": "ya_make_try_1", "source": "ya_phase"})
            self.assertEqual(labels["github.event_name"], "pull_request_target")
            self.assertEqual(labels["github.event.number"], 53660)
            self.assertEqual(labels["github.event.action"], "synchronize")
            self.assertEqual(labels["github.event.pull_request.number"], 53660)
            self.assertEqual(labels["github.event.pull_request.head.sha"], "abc123def")
            self.assertEqual(labels["github.event.pull_request.base.ref"], "main")
            self.assertEqual(labels["github.event.pull_request.labels"], ["ci"])
            self.assertEqual(labels["github.workflow"], "PR-check")
            self.assertEqual(labels["github.job"], "build_and_test")
            self.assertEqual(labels["github.run_attempt"], 2)
            self.assertNotIn("github.event.pull_request.body", labels)
            self.assertEqual(record["pr_number"], 53660)
            self.assertEqual(record["event_name"], "pull_request_target")
            self.assertEqual(record["commit"], "abc123def")
            self.assertEqual(record["labels"]["github.event.number"], 53660)
        finally:
            for key, value in old.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


class ParseDatetimeTest(unittest.TestCase):
    def test_epoch_strings_and_iso(self):
        self.assertEqual(
            parse_datetime("1000"),
            datetime.fromtimestamp(1000, tz=timezone.utc),
        )
        self.assertEqual(
            parse_datetime("1010.5"),
            datetime.fromtimestamp(1010.5, tz=timezone.utc),
        )
        self.assertEqual(
            parse_datetime("1726900000000"),
            datetime.fromtimestamp(1726900000, tz=timezone.utc),
        )
        self.assertEqual(
            parse_datetime("2026-09-21T10:00:00Z"),
            datetime(2026, 9, 21, 10, 0, tzinfo=timezone.utc),
        )


class RecordApiTest(unittest.TestCase):
    def test_build_record_duration_from_epochs(self):
        record = build_track_record(
            "ya_make_try_1",
            {
                "source": "ya_phase",
                "started_epoch": "1000",
                "finished_epoch": "1010.5",
                "conclusion": "success",
                "ya_attempt": 1,
            },
        )
        self.assertEqual(record["name"], "ya_make_try_1")
        self.assertEqual(record["value"], 10500.0)
        self.assertEqual(record["unit"], "ms")
        self.assertEqual(record["labels"]["ya_attempt"], 1)
        self.assertNotIn("duration_ms", record["labels"])

    def test_duration_ms_not_copied_into_labels(self):
        record = build_track_record(
            "ya_make_try_1",
            {"source": "ya_phase", "duration_ms": 1500, "cache_mode": "none"},
        )
        self.assertEqual(record["value"], 1500.0)
        self.assertEqual(record["labels"], {"cache_mode": "none"})

    def test_finished_at_maps_to_duration(self):
        record = build_track_record(
            "ya_make_try_1",
            {
                "source": "ya_phase",
                "started_epoch": "1000",
                "finished_at": "1970-01-01T00:16:42Z",
            },
        )
        self.assertEqual(record["value"], 2000.0)

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

    def test_timed_records_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            with self.assertRaises(RuntimeError):
                with timed("boom", file=path, source="test"):
                    raise RuntimeError("nope")
            with open(path, encoding="utf-8") as handle:
                record = json.loads(handle.readline())
            self.assertEqual(record["name"], "boom")
            self.assertEqual(record["conclusion"], "failure")
            self.assertGreaterEqual(record["value"], 0)

    def test_append_record_creates_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "out.jsonl")
            append_record(path, {"name": "x"})
            self.assertTrue(os.path.exists(path))


class TrackApiTest(unittest.TestCase):
    def test_name_and_properties_json(self):
        record = build_track_record(
            "ya_make_try_1",
            {
                "kind": "duration",
                "source": "ya_phase",
                "started_epoch": "1000",
                "finished_epoch": "1010",
                "conclusion": "success",
                "cache_mode": "dist_cache",
                "ya_attempt": 1,
            },
        )
        self.assertEqual(record["name"], "ya_make_try_1")
        self.assertEqual(record["kind"], "duration")
        self.assertEqual(record["source"], "ya_phase")
        self.assertEqual(record["value"], 10000.0)
        self.assertEqual(record["labels"]["cache_mode"], "dist_cache")
        self.assertEqual(record["labels"]["ya_attempt"], 1)

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
                        "--duration-sec",
                        "2.5",
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

    def test_cli_track_json(self):
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
                        "--json",
                        json.dumps({"kind": "gauge", "value": 42, "unit": "bytes", "source": "clean_build", "cache_mode": "none"}),
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

        import ci_metrics as client

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

    def test_analytics_client_default_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            analytics = Analytics(file=path, source="ya_phase")
            analytics.track("graph_compare", {"value": 5, "conclusion": "success"})
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], "graph_compare")
            self.assertEqual(row["source"], "ya_phase")
            self.assertEqual(row["value"], 5.0)

    def test_start_send_computes_duration(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import ci_metrics as client

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
                            "--attr",
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

    def test_cli_build_info_json_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            snap = os.path.join(tmp, "modules.json")
            with open(snap, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "components": [
                            {"name": "ydb/apps/ydbd", "duration_ms": 12000},
                            {"name": "ydb/core/tablet", "duration_ms": 800},
                        ]
                    },
                    handle,
                )
            self.assertEqual(
                main(
                    [
                        "track",
                        BUILD_INFO_NAME,
                        "--file",
                        path,
                        "--source",
                        "nightly_build",
                        "--json-file",
                        snap,
                    ]
                ),
                0,
            )
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], BUILD_INFO_NAME)
            self.assertEqual(row["kind"], "info")
            self.assertEqual(row["source"], "nightly_build")
            self.assertNotIn("value", row)
            self.assertEqual(row["labels"]["payload"]["components"][0]["name"], "ydb/apps/ydbd")

    def test_send_after_start_writes_sibling_build_info(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import ci_metrics as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "ci_metrics.jsonl")
                snap = os.path.join(tmp, "modules.json")
                with open(snap, "w", encoding="utf-8") as handle:
                    json.dump({"nodes": [{"name": "a.cpp", "duration_ms": 10}]}, handle)
                start("ydbd_clean_build", file=path, source="clean_build", started_epoch="1000")
                self.assertEqual(
                    main(
                        [
                            "send",
                            "--file",
                            path,
                            "--conclusion",
                            "success",
                            "--finished-epoch",
                            "1005",
                            "--json-file",
                            snap,
                        ]
                    ),
                    0,
                )
                with open(path, encoding="utf-8") as handle:
                    rows = [json.loads(line) for line in handle if line.strip()]
                names = {row["name"]: row for row in rows}
                self.assertEqual(names["ydbd_clean_build"]["kind"], "duration")
                self.assertEqual(names["ydbd_clean_build"]["value"], 5000.0)
                self.assertNotIn("payload", names["ydbd_clean_build"].get("labels") or {})
                self.assertEqual(names[BUILD_INFO_NAME]["kind"], "info")
                self.assertEqual(names[BUILD_INFO_NAME]["source"], "clean_build")
                self.assertEqual(names[BUILD_INFO_NAME]["labels"]["payload"]["nodes"][0]["name"], "a.cpp")
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_track_does_not_end_open_spans(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            start("ydbd_cached_build", file=path, source="nightly_build")
            track("ydb/foo.cpp", {"node_kind": "Compile"}, file=path, kind="duration", value=1, source="nightly_build")
            pending = read_pending_spans(path)
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["name"], "ydbd_cached_build")
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual([row["name"] for row in rows], ["ydb/foo.cpp"])

    def test_send_json_file_without_start(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None):
            sends.append(path)
            return 0

        import ci_metrics as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "ci_metrics.jsonl")
                snap = os.path.join(tmp, "modules.json")
                with open(snap, "w", encoding="utf-8") as handle:
                    json.dump({"modules": [{"name": "ydbd"}]}, handle)
                self.assertEqual(
                    main(["send", "--file", path, "--source", "other_wf", "--json-file", snap]),
                    0,
                )
                with open(path, encoding="utf-8") as handle:
                    row = json.loads(handle.readline())
                self.assertEqual(row["name"], BUILD_INFO_NAME)
                self.assertEqual(row["kind"], "info")
                self.assertEqual(row["source"], "other_wf")
                self.assertEqual(row["labels"]["payload"]["modules"][0]["name"], "ydbd")
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_track_info_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            analytics = Analytics(file=path, source="build_bloat")
            analytics.track(
                BUILD_INFO_NAME,
                {"payload": {"cpp_compilation_times": [{"path": "a.cpp", "time_s": 1.5}]}},
                kind="info",
            )
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["kind"], "info")
            self.assertEqual(row["labels"]["payload"]["cpp_compilation_times"][0]["path"], "a.cpp")

    def test_cpp_json_file_nests_under_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_metrics.jsonl")
            snap = os.path.join(tmp, "output.json")
            with open(snap, "w", encoding="utf-8") as handle:
                json.dump({"total_compilation_time": 3.5, "cpp_compilation_times": [{"path": "a.cpp", "time_s": 2.0}]}, handle)
            self.assertEqual(
                main(["track", "build_info", "--file", path, "--source", "build_bloat", "--json-file", snap]),
                0,
            )
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["kind"], "info")
            self.assertEqual(row["labels"]["payload"]["total_compilation_time"], 3.5)
            self.assertNotIn("cpp_compilation_times", row["labels"])


class ParseLabelsTest(unittest.TestCase):
    def test_key_values_and_extra_json(self):
        labels = parse_labels(["cache_mode=none", "ya_attempt=2"], '{"nproc": 8}')
        self.assertEqual(labels["cache_mode"], "none")
        self.assertEqual(labels["ya_attempt"], "2")
        self.assertEqual(labels["nproc"], 8)

    def test_invalid_extra_kept_raw(self):
        labels = parse_labels([], "not-json")
        self.assertEqual(labels["extra"], "not-json")


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
            "boot_time": 1700000000,
            "cpu_count": 8,
            "cpu_model": "Test CPU",
            "mem_total_bytes": 16 * 1024**3,
            "disk_total_bytes": 100 * 1024**3,
            "disks": [{"name": "sda", "size_bytes": 100 * 1024**3}],
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
        self.assertEqual(first["boot_time"], 1700000000)
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

        import ci_metrics as client

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


if __name__ == "__main__":
    unittest.main()

