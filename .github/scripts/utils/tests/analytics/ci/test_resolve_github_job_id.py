#!/usr/bin/env python3

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "analytics"))

import unittest

from resolve_github_job_id import list_run_jobs, pick_job, resolve_github_job


class PickJobTest(unittest.TestCase):
    def test_same_preset_different_runners(self):
        jobs = [
            {"id": 1, "name": "Build and test relwithdebinfo on main", "runner_name": "runner-a"},
            {"id": 2, "name": "Build and test relwithdebinfo on stable", "runner_name": "runner-b"},
        ]
        self.assertEqual(pick_job(jobs, "runner-b")["id"], 2)
        self.assertIsNone(pick_job(jobs, "runner-missing"))
        self.assertIsNone(pick_job(jobs, ""))

    def test_prefers_in_progress_over_earlier_job_on_same_runner(self):
        jobs = [
            {
                "id": 1,
                "runner_name": "same",
                "status": "completed",
                "started_at": "2026-09-25T10:00:00Z",
            },
            {
                "id": 2,
                "runner_name": "same",
                "status": "in_progress",
                "started_at": "2026-09-25T12:00:00Z",
            },
        ]
        self.assertEqual(pick_job(jobs, "same")["id"], 2)

    def test_prefers_the_job_whose_step_is_running(self):
        jobs = [
            {
                "id": 1,
                "runner_name": "same",
                "status": "in_progress",
                "started_at": "2026-09-25T12:00:00Z",
                "steps": [{"name": "done", "status": "completed"}],
            },
            {
                "id": 2,
                "runner_name": "same",
                "status": "in_progress",
                "started_at": "2026-09-25T11:00:00Z",
                "steps": [{"name": "build", "status": "in_progress"}],
            },
        ]
        self.assertEqual(pick_job(jobs, "same")["id"], 2)

    def test_a_finished_job_on_the_same_runner_never_wins(self):
        """Runner names are reused; a completed job is a previous tenant."""
        jobs = [
            {
                "id": 1,
                "runner_name": "same",
                "status": "completed",
                "started_at": "2026-09-25T10:00:00Z",
            }
        ]
        self.assertIsNone(pick_job(jobs, "same"))


class ResolveRetryTest(unittest.TestCase):
    def test_retries_until_the_job_becomes_visible(self):
        pages = [
            {"jobs": []},
            {"jobs": [{"id": 5, "runner_name": "mine", "status": "completed"}]},
            {"jobs": [{"id": 6, "runner_name": "mine", "status": "in_progress"}]},
        ]
        calls = {"n": 0}

        def get_json(url, params, *, token=None):
            payload = pages[min(calls["n"], len(pages) - 1)]
            calls["n"] += 1
            return payload

        slept = []
        job = resolve_github_job(
            "mine",
            repository="ydb-platform/ydb",
            run_id="5",
            token="t",
            get_json=get_json,
            sleep=slept.append,
        )
        self.assertEqual(job["id"], 6)
        self.assertEqual(len(slept), 2, "slept once per miss")

    def test_gives_up_after_the_attempt_budget(self):
        def get_json(url, params, *, token=None):
            return {"jobs": [{"id": 1, "runner_name": "mine", "status": "completed"}]}

        slept = []
        job = resolve_github_job(
            "mine",
            repository="ydb-platform/ydb",
            run_id="5",
            token="t",
            get_json=get_json,
            attempts=3,
            sleep=slept.append,
        )
        self.assertIsNone(job)
        self.assertEqual(len(slept), 2)

    def test_listing_error_is_retried(self):
        calls = {"n": 0}

        def get_json(url, params, *, token=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("GitHub API 502")
            return {"jobs": [{"id": 7, "runner_name": "mine", "status": "in_progress"}]}

        job = resolve_github_job(
            "mine",
            repository="ydb-platform/ydb",
            run_id="5",
            token="t",
            get_json=get_json,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(job["id"], 7)


class PaginationTest(unittest.TestCase):
    def test_lists_two_pages_and_picks_runner(self):
        pages = {
            1: {"jobs": [{"id": 1, "name": "other", "runner_name": "a"}, {"id": 2, "name": "setup", "runner_name": "b"}]},
            2: {"jobs": [{"id": 9, "name": "Build and test relwithdebinfo", "runner_name": "this-runner"}]},
        }

        def get_json(url, params, *, token=None):
            return pages[params["page"]]

        jobs = list_run_jobs("ydb-platform/ydb", "1", "tok", get_json=get_json, per_page=2)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(pick_job(jobs, "this-runner")["id"], 9)

    def test_resolve_uses_injected_fetch(self):
        def get_json(url, params, *, token=None):
            return {"jobs": [{"id": 77, "name": "Build and test release-asan", "runner_name": "mine"}]}

        job = resolve_github_job(
            "mine",
            repository="ydb-platform/ydb",
            run_id="5",
            token="t",
            get_json=get_json,
            sleep=lambda _seconds: None,
        )
        self.assertEqual(job["id"], 77)


if __name__ == "__main__":
    unittest.main()
