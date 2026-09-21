#!/usr/bin/env python3
"""Unit tests for GitHub job-metrics collector helpers."""

from __future__ import annotations

import os
import unittest

from export_github_job_metrics import DEFAULT_WORKFLOWS, resolve_workflows, split_workflows


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

    def test_default_list(self):
        old = os.environ.get("CI_METRICS_WORKFLOW")
        try:
            os.environ.pop("CI_METRICS_WORKFLOW", None)
            self.assertEqual(resolve_workflows(None), list(DEFAULT_WORKFLOWS))
        finally:
            if old is None:
                os.environ.pop("CI_METRICS_WORKFLOW", None)
            else:
                os.environ["CI_METRICS_WORKFLOW"] = old

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


if __name__ == "__main__":
    unittest.main()
