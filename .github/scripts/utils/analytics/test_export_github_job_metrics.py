#!/usr/bin/env python3
"""Unit tests for GitHub job-metrics collector helpers."""

from __future__ import annotations

import os
import unittest

from export_github_job_metrics import (
    ALL_WORKFLOWS,
    DEFAULT_WORKFLOWS,
    attach_pull_requests,
    is_all_workflows,
    pull_refs_from_commit_pulls,
    pull_requests_have_target,
    resolve_workflows,
    split_workflows,
    workflow_file_name,
    workflows_from_github_payload,
)


class SplitWorkflowsTest(unittest.TestCase):
    def test_comma_and_repeats(self):
        self.assertEqual(
            split_workflows(["pr_check.yml,nightly_build.yml", "pr_check.yml"]),
            ["pr_check.yml", "nightly_build.yml"],
        )

    def test_empty(self):
        self.assertEqual(split_workflows(None), [])
        self.assertEqual(split_workflows(""), [])
        self.assertEqual(split_workflows("  ,  "), [])


class ResolveWorkflowsTest(unittest.TestCase):
    def test_explicit_wins(self):
        self.assertEqual(resolve_workflows(["ydbd_clean_build.yml"]), ["ydbd_clean_build.yml"])

    def test_default_is_all(self):
        old = os.environ.get("CI_METRICS_WORKFLOW")
        try:
            os.environ.pop("CI_METRICS_WORKFLOW", None)
            self.assertEqual(resolve_workflows(None), [ALL_WORKFLOWS])
            self.assertEqual(resolve_workflows(None), list(DEFAULT_WORKFLOWS))
            self.assertTrue(is_all_workflows(resolve_workflows(None)))
        finally:
            if old is None:
                os.environ.pop("CI_METRICS_WORKFLOW", None)
            else:
                os.environ["CI_METRICS_WORKFLOW"] = old

    def test_explicit_all(self):
        self.assertTrue(is_all_workflows(["all"]))
        self.assertTrue(is_all_workflows(["ALL"]))
        self.assertFalse(is_all_workflows(["pr_check.yml"]))

    def test_env_override(self):
        old = os.environ.get("CI_METRICS_WORKFLOW")
        try:
            os.environ["CI_METRICS_WORKFLOW"] = "pr_check.yml,nightly_build.yml"
            self.assertEqual(resolve_workflows(None), ["pr_check.yml", "nightly_build.yml"])
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


class ListWorkflowsTest(unittest.TestCase):
    def test_file_name_from_path(self):
        self.assertEqual(workflow_file_name(".github/workflows/pr_check.yml"), "pr_check.yml")
        self.assertEqual(workflow_file_name("run_tests.yml"), "run_tests.yml")

    def test_skips_disabled_and_sorts(self):
        self.assertEqual(
            workflows_from_github_payload(
                {
                    "workflows": [
                        {"path": ".github/workflows/run_tests.yml", "state": "active"},
                        {"path": ".github/workflows/pr_check.yml", "state": "active"},
                        {"path": ".github/workflows/old.yml", "state": "disabled_manually"},
                        {"path": "README.md", "state": "active"},
                    ]
                }
            ),
            ["pr_check.yml", "run_tests.yml"],
        )


if __name__ == "__main__":
    unittest.main()
