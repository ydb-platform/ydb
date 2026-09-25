#!/usr/bin/env python3
"""Unit tests for GitHub job-metrics collector helpers."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import os
import unittest

from datetime import datetime, timedelta, timezone

from github_actions.export_github_job_metrics import (
    already_exported,
    attach_pull_requests,
    completed_since,
    pull_refs_from_commit_pulls,
    pull_requests_have_target,
    selected_workflows,
)


class WorkflowNamesTest(unittest.TestCase):
    def test_explicit_and_repeats(self):
        self.assertEqual(
            selected_workflows(["pr_check.yml,nightly_build.yml", "pr_check.yml"]),
            ["pr_check.yml", "nightly_build.yml"],
        )
        self.assertEqual(selected_workflows(["run_tests.yml"]), ["run_tests.yml"])

    def test_default_is_all(self):
        old = os.environ.get("CI_METRICS_WORKFLOW")
        try:
            os.environ.pop("CI_METRICS_WORKFLOW", None)
            self.assertEqual(selected_workflows(None), ["all"])
            self.assertEqual(selected_workflows(["ALL"]), ["all"])
        finally:
            if old is None:
                os.environ.pop("CI_METRICS_WORKFLOW", None)
            else:
                os.environ["CI_METRICS_WORKFLOW"] = old


class PullRefsTest(unittest.TestCase):
    def test_keeps_target_branch(self):
        self.assertEqual(
            pull_refs_from_commit_pulls(
                [{"number": 42, "base": {"ref": "main"}, "head": {"ref": "feature"}}]
            ),
            [{"number": 42, "base": {"ref": "main"}}],
        )

    def test_skips_non_dicts(self):
        self.assertEqual(pull_refs_from_commit_pulls("nope"), [])

    def test_attach_keeps_existing_pulls(self):
        run = {
            "event": "pull_request_target",
            "head_sha": "abc",
            "pull_requests": [{"number": 1, "base": {"ref": "stable-26"}}],
        }
        self.assertIs(attach_pull_requests("ydb-platform", "ydb", run), run)

    def test_target_branch_present(self):
        self.assertTrue(pull_requests_have_target([{"number": 1, "base": {"ref": "main"}}]))
        self.assertFalse(pull_requests_have_target([{"number": 1}]))
        self.assertFalse(pull_requests_have_target([]))

    def test_push_is_not_enriched(self):
        run = {"event": "push", "head_sha": "abc", "head_branch": "main"}
        self.assertIs(attach_pull_requests("ydb-platform", "ydb", run), run)


class CompletedSinceTest(unittest.TestCase):
    def test_cold_start_uses_hours(self):
        since = completed_since(2, None)
        delta = datetime.now(timezone.utc) - since
        self.assertGreater(delta.total_seconds(), 2 * 3600 - 5)
        self.assertLess(delta.total_seconds(), 2 * 3600 + 5)

    def test_watermark_is_last_export_minus_30m(self):
        last = datetime.now(timezone.utc) - timedelta(minutes=10)
        since = completed_since(2, last)
        self.assertLess(abs((since - (last - timedelta(minutes=30))).total_seconds()), 2)


class AlreadyExportedTest(unittest.TestCase):
    def test_skips_known_attempt_and_keeps_a_new_one(self):
        exported = {(10, 1)}
        self.assertTrue(already_exported(10, 1, exported))
        self.assertTrue(already_exported("10", "1", exported))
        self.assertFalse(already_exported(10, 2, exported))
        self.assertFalse(already_exported(12, 1, exported))
        self.assertFalse(already_exported(None, 1, exported))


if __name__ == "__main__":
    unittest.main()
