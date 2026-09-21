#!/usr/bin/env python3
"""Unit tests for CI PR-check stage row builders. No YDB required."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from ci_pr_check_stages import (
    DEFAULT_TABLE_PATH,
    PRIMARY_KEYS,
    build_create_table_sql,
    github_env_defaults,
    guess_build_preset,
    normalize_stage_row,
    rows_from_jsonl,
    rows_from_workflow_run,
)
from ci_stage_timer import append_record, build_record, parse_args


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


class NormalizeRowTest(unittest.TestCase):
    def test_requires_core_fields(self):
        self.assertIsNone(normalize_stage_row({"stage_name": "job"}))
        self.assertIsNone(
            normalize_stage_row(
                {
                    "stage_kind": "github_job",
                    "stage_name": "job",
                    "started_at": "2026-09-21T10:00:00Z",
                }
            )
        )

    def test_epoch_and_duration(self):
        row = normalize_stage_row(
            {
                "stage_kind": "ya_phase",
                "stage_name": "ya_make_try_1",
                "started_at": 1726900000,
                "run_id": 123,
                "duration_ms": 45000,
                "conclusion": "success",
            }
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["run_id"], 123)
        self.assertEqual(row["duration_ms"], 45000)
        self.assertEqual(row["date"], datetime.fromtimestamp(1726900000, tz=timezone.utc).date())
        self.assertEqual(row["github_job_id"], 0)


class JsonlRowsTest(unittest.TestCase):
    def test_skips_bad_lines_and_applies_defaults(self):
        payload = "\n".join(
            [
                "not-json",
                json.dumps(
                    {
                        "stage_kind": "ya_phase",
                        "stage_name": "graph_compare",
                        "started_at": "2026-09-21T10:00:00Z",
                        "duration_ms": 1200,
                    }
                ),
                "",
            ]
        )
        rows = rows_from_jsonl(io.StringIO(payload), defaults={"run_id": 99, "build_preset": "relwithdebinfo"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["run_id"], 99)
        self.assertEqual(rows[0]["build_preset"], "relwithdebinfo")
        self.assertEqual(rows[0]["stage_name"], "graph_compare")


class WorkflowRunRowsTest(unittest.TestCase):
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
        rows = rows_from_workflow_run(run, jobs)
        kinds = {(row["stage_kind"], row["stage_name"]) for row in rows}
        self.assertIn(("github_job", "job"), kinds)
        self.assertIn(("github_job", "queue"), kinds)
        self.assertIn(("github_step", "Checkout"), kinds)
        self.assertIn(("github_step", "ya build and test"), kinds)
        self.assertNotIn(("github_step", "skipped step"), kinds)

        job_row = next(row for row in rows if row["stage_name"] == "job")
        self.assertEqual(job_row["run_id"], 555)
        self.assertEqual(job_row["github_job_id"], 777)
        self.assertEqual(job_row["pr_number"], 42)
        self.assertEqual(job_row["branch"], "main")
        self.assertEqual(job_row["build_preset"], "relwithdebinfo")
        self.assertEqual(job_row["duration_ms"], 60 * 60 * 1000)
        self.assertEqual(job_row["queued_ms"], 5 * 60 * 1000)

        queue_row = next(row for row in rows if row["stage_name"] == "queue")
        self.assertEqual(queue_row["duration_ms"], 5 * 60 * 1000)


class SchemaTest(unittest.TestCase):
    def test_create_sql_has_pk_and_ttl(self):
        sql = build_create_table_sql(DEFAULT_TABLE_PATH)
        self.assertIn(DEFAULT_TABLE_PATH, sql)
        self.assertIn("STORE = COLUMN", sql)
        self.assertIn("TTL = Interval", sql)
        self.assertIn("ON started_at", sql)
        for key in PRIMARY_KEYS:
            self.assertIn(f"`{key}`", sql)


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


class StageTimerTest(unittest.TestCase):
    def test_append_jsonl(self):
        args = parse_args(
            [
                "append",
                "--file",
                "unused.jsonl",
                "--stage-name",
                "ya_make_try_1",
                "--started-epoch",
                "1000",
                "--finished-epoch",
                "1010.5",
                "--conclusion",
                "success",
                "--ya-attempt",
                "1",
            ]
        )
        record = build_record(args)
        self.assertEqual(record["stage_name"], "ya_make_try_1")
        self.assertEqual(record["duration_ms"], 10500)
        self.assertEqual(record["ya_attempt"], 1)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ci_stages.jsonl")
            append_record(path, record)
            with open(path, encoding="utf-8") as handle:
                loaded = json.loads(handle.readline())
            self.assertEqual(loaded["stage_name"], "ya_make_try_1")


if __name__ == "__main__":
    unittest.main()
