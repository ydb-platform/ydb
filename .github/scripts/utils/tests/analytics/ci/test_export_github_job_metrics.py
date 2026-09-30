#!/usr/bin/env python3
"""Unit tests for GitHub job-metrics collector helpers."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import os
import unittest
from unittest.mock import MagicMock, patch

from datetime import datetime, timedelta, timezone

from github_actions import ci_metrics
from github_actions import export_github_job_metrics
from github_actions import state
from github_actions.github_api import NotFound, RateLimitExhausted
from github_actions.export_github_job_metrics import (
    already_exported,
    attach_pull_requests,
    completed_since,
    metrics_from_workflow_run,
    open_runs_to_save,
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


class CreatedUntilTest(unittest.TestCase):
    def test_iter_workflow_runs_builds_a_closed_created_range(self):
        seen = {}

        def fake_get(_url, params=None, **_kwargs):
            seen["params"] = params
            return {"workflow_runs": []}

        start = datetime(2026, 8, 29, tzinfo=timezone.utc)
        end = datetime(2026, 9, 3, tzinfo=timezone.utc)
        with patch.object(export_github_job_metrics, "github_get", fake_get):
            list(
                export_github_job_metrics.iter_workflow_runs(
                    "ydb-platform",
                    "ydb",
                    "pr_check.yml",
                    start,
                    created_until=end,
                )
            )
        self.assertEqual(seen["params"]["created"], "2026-08-29T00:00:00Z..2026-09-03T00:00:00Z")


class CompletedSinceTest(unittest.TestCase):
    def test_cold_start_uses_hours(self):
        since = completed_since(2, None)
        delta = datetime.now(timezone.utc) - since
        self.assertGreater(delta.total_seconds(), 2 * 3600 - 5)
        self.assertLess(delta.total_seconds(), 2 * 3600 + 5)

    def test_watermark_is_last_export_minus_30m_even_when_old(self):
        """No lookback floor: an export that was down for days resumes where it stopped."""
        for last in (
            datetime.now(timezone.utc) - timedelta(minutes=10),
            datetime.now(timezone.utc) - timedelta(days=3),
        ):
            since = completed_since(2, last)
            self.assertLess(abs((since - (last - timedelta(minutes=30))).total_seconds()), 2)

    def test_unreadable_watermark_refuses_to_export(self):
        with patch.object(export_github_job_metrics, "load_watermark", return_value=(None, False)), patch.object(
            export_github_job_metrics, "workflows_to_export", return_value=["pr_check.yml"]
        ), patch.object(export_github_job_metrics, "save_watermark") as save:
            self.assertEqual(export_github_job_metrics.main([]), 1)
        save.assert_not_called()

    def test_unreadable_open_runs_refuse_to_export(self):
        with self._missing_state_patch(open_ok=False), patch.object(
            export_github_job_metrics, "save_open_runs"
        ) as save_open, patch.object(
            export_github_job_metrics, "save_watermark"
        ) as save_wm:
            self.assertEqual(export_github_job_metrics.main([]), 1)
        save_open.assert_not_called()
        save_wm.assert_not_called()

    def _missing_state_patch(self, *, open_ok=True, failed_ok=True):
        return patch.multiple(
            export_github_job_metrics,
            workflows_to_export=lambda *a, **k: ["pr_check.yml"],
            load_watermark=lambda *a, **k: (None, True),
            exported_run_ids=lambda *a, **k: set(),
            exported_job_ids=lambda *a, **k: set(),
            load_failed_runs=lambda *a, **k: ({}, failed_ok),
            load_open_runs=lambda *a, **k: ([], open_ok),
        )


class OpenRunsToSaveTest(unittest.TestCase):
    def test_drops_finished_when_the_list_succeeded(self):
        self.assertEqual(
            open_runs_to_save([(1, 1)], [(2, 1)], [(9, 1)], False, set()),
            [(1, 1), (2, 1)],
        )

    def test_list_failure_keeps_previous_open_runs(self):
        self.assertEqual(
            open_runs_to_save([(1, 1)], [], [(1, 1), (4, 1)], True, set()),
            [(1, 1), (4, 1)],
        )

    def test_list_failure_skips_runs_already_in_the_table(self):
        self.assertEqual(
            open_runs_to_save([], [], [(4, 1), (5, 1)], True, {(5, 1)}),
            [(4, 1)],
        )


class CancelledJobExportTest(unittest.TestCase):
    def test_exports_started_steps_on_a_cancelled_job(self):
        run = {
            "id": 20,
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "cancelled",
            "event": "pull_request_target",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "feature",
            "created_at": "2026-09-28T10:00:00Z",
            "html_url": "https://example.test/run/20",
            "pull_requests": [{"number": 1, "base": {"ref": "main"}}],
        }
        jobs = [
            {
                "id": 201,
                "name": "Build and test relwithdebinfo",
                "created_at": "2026-09-28T10:00:00Z",
                "started_at": "2026-09-28T10:02:00Z",
                "completed_at": "2026-09-28T10:10:00Z",
                "conclusion": "cancelled",
                "steps": [
                    {
                        "name": "Checkout",
                        "started_at": "2026-09-28T10:02:00Z",
                        "completed_at": "2026-09-28T10:03:00Z",
                        "conclusion": "success",
                    },
                    {
                        "name": "Build and test",
                        "started_at": "2026-09-28T10:03:00Z",
                        "completed_at": "2026-09-28T10:10:00Z",
                        "conclusion": "cancelled",
                    },
                    {
                        "name": "Post Cancel",
                        "started_at": None,
                        "completed_at": None,
                        "conclusion": "skipped",
                    },
                ],
            },
            {
                "id": 202,
                "name": "Build and test release-asan",
                "created_at": "2026-09-28T10:00:00Z",
                "started_at": None,
                "completed_at": None,
                "conclusion": "cancelled",
                "steps": [
                    {
                        "name": "Checkout",
                        "started_at": None,
                        "completed_at": None,
                        "conclusion": "skipped",
                    },
                ],
            },
        ]
        rows = metrics_from_workflow_run(run, jobs)
        names = {(row["github_job_id"], row["name"], row.get("conclusion")) for row in rows}
        self.assertIn((201, "job", "cancelled"), names)
        self.assertIn((201, "queue", None), names)
        self.assertIn((201, "Checkout", "success"), names)
        self.assertIn((201, "Build and test", "cancelled"), names)
        self.assertFalse(any(row["name"] == "Post Cancel" for row in rows))
        self.assertFalse(any(row["github_job_id"] == 202 for row in rows))

    def test_held_cancelled_run_is_exported(self):
        run = {
            "id": 21,
            "run_attempt": 1,
            "status": "completed",
            "conclusion": "cancelled",
            "event": "pull_request_target",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "feature",
            "created_at": "2026-09-28T10:00:00Z",
            "html_url": "https://example.test/run/21",
            "pull_requests": [{"number": 1, "base": {"ref": "main"}}],
        }
        jobs = [
            {
                "id": 210,
                "name": "Build and test relwithdebinfo",
                "created_at": "2026-09-28T10:00:00Z",
                "started_at": "2026-09-28T10:02:00Z",
                "completed_at": "2026-09-28T10:04:00Z",
                "conclusion": "cancelled",
                "steps": [],
            }
        ]

        def fake_fetch(_org, _repo, _run_id, attempt=None):
            return run

        def fake_jobs(_org, _repo, _run_id, per_page=100, attempt=None):
            return jobs

        with patch.object(export_github_job_metrics, "fetch_run", fake_fetch), patch.object(
            export_github_job_metrics, "list_run_jobs", fake_jobs
        ):
            rows, held = export_github_job_metrics.export_held_runs(
                "ydb-platform", "ydb", [(21, 1)], [], set(), {}
            )
        self.assertEqual(held, [])
        job_rows = [row for row in rows if row.get("name") == "job"]
        self.assertEqual(len(job_rows), 1)
        self.assertEqual(job_rows[0]["conclusion"], "cancelled")
        self.assertEqual(job_rows[0]["github_job_id"], 210)


class AlreadyExportedTest(unittest.TestCase):
    def test_skips_known_attempt_and_keeps_a_new_one(self):
        exported = {(10, 1)}
        self.assertTrue(already_exported(10, 1, exported))
        self.assertTrue(already_exported("10", "1", exported))
        self.assertFalse(already_exported(10, 2, exported))
        self.assertFalse(already_exported(12, 1, exported))
        self.assertFalse(already_exported(None, 1, exported))

    def test_rerun_keeps_a_new_job_on_the_same_attempt(self):
        run = {
            "id": 10,
            "run_attempt": 1,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
            "html_url": "https://example.test/run/10",
        }
        jobs = [
            {
                "id": 100,
                "name": "Build and test relwithdebinfo",
                "started_at": "2026-09-28T10:00:00Z",
                "completed_at": "2026-09-28T10:05:00Z",
                "conclusion": "success",
                "steps": [],
            },
            {
                "id": 200,
                "name": "Build and test relwithdebinfo",
                "started_at": "2026-09-28T11:00:00Z",
                "completed_at": "2026-09-28T11:05:00Z",
                "conclusion": "success",
                "steps": [],
            },
        ]
        rows = metrics_from_workflow_run(run, jobs, skip_job_ids={100})
        job_rows = [row for row in rows if row.get("name") == "job"]
        self.assertEqual([row["github_job_id"] for row in job_rows], [200])


class UpsertMissingColumnsTest(unittest.TestCase):
    def test_absent_dimensions_become_explicit_nulls(self):
        seen = {}
        original = ci_metrics.collector_upsert_metrics

        def fake(_wrapper, rows, **_kwargs):
            seen["rows"] = rows
            return len(rows)

        ci_metrics.collector_upsert_metrics = fake
        try:
            ci_metrics.upsert_metrics(
                object(),
                [{
                    "date": datetime.now(timezone.utc).date(),
                    "event_ts": datetime.now(timezone.utc),
                    "run_id": 7,
                    "github_job_id": 70,
                    "run_attempt": 1,
                    "source": "ya_phase",
                    "name": "ya_make_try_1",
                    "kind": "duration",
                    "span_id": "abc",
                    "labels": "{}",
                    "exported_at": datetime.now(timezone.utc),
                }],
            )
        finally:
            ci_metrics.collector_upsert_metrics = original
        row = seen["rows"][0]
        self.assertIsNone(row["workflow"])
        self.assertIsNone(row["job_name"])
        self.assertIsNone(row["run_url"])


class HeldRunTest(unittest.TestCase):
    """A run that finished while it was held must be read at its stored attempt."""

    def _run(self, attempt, status="completed"):
        return {
            "id": 10,
            "run_attempt": attempt,
            "status": status,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
            "html_url": "https://example.test/run/10",
        }

    def test_fetches_the_stored_attempt_not_the_latest(self):
        calls = []

        def fake_fetch(org, repo, run_id, attempt=None):
            calls.append(("run", run_id, attempt))
            return self._run(attempt or 9)

        def fake_jobs(org, repo, run_id, per_page=100, attempt=None):
            calls.append(("jobs", run_id, attempt))
            return []

        with patch.object(export_github_job_metrics, "fetch_run", fake_fetch), patch.object(
            export_github_job_metrics, "list_run_jobs", fake_jobs
        ):
            rows, held = export_github_job_metrics.export_held_runs(
                "ydb-platform", "ydb", [(10, 2)], [], set(), {}
            )
        self.assertEqual(rows, [])
        self.assertEqual(held, [])
        self.assertEqual(calls, [("run", 10, 2), ("jobs", 10, 2)])

    def test_missing_run_is_dropped_not_retried_forever(self):
        failed = {(10, 1): 1}
        with patch.object(
            export_github_job_metrics, "fetch_run", side_effect=NotFound("gone")
        ):
            rows, held = export_github_job_metrics.export_held_runs(
                "ydb-platform", "ydb", [(10, 1)], [], set(), failed
            )
        self.assertEqual((rows, held), ([], []))
        self.assertEqual(failed, {})

    def test_transient_failure_is_queued_for_retry(self):
        failed = {}
        with patch.object(
            export_github_job_metrics, "fetch_run", side_effect=RuntimeError("502")
        ):
            _rows, held = export_github_job_metrics.export_held_runs(
                "ydb-platform", "ydb", [(10, 1)], [], set(), failed
            )
        self.assertEqual(held, [(10, 1)])
        self.assertEqual(failed, {(10, 1): 1})

    def test_retry_budget_is_bounded(self):
        failed = {(10, 1): state.MAX_RETRY_ATTEMPTS - 1}
        with patch.object(
            export_github_job_metrics, "fetch_run", side_effect=RuntimeError("502")
        ):
            _rows, held = export_github_job_metrics.export_held_runs(
                "ydb-platform", "ydb", [(10, 1)], [], set(), failed
            )
        self.assertEqual(held, [])
        self.assertEqual(failed, {})


class CollectRowsFailureTest(unittest.TestCase):
    def test_unlistable_run_is_queued_instead_of_dropped(self):
        failed = {}
        with patch.object(
            export_github_job_metrics,
            "iter_workflow_runs",
            return_value=[{"id": 5, "run_attempt": 1}],
        ), patch.object(
            export_github_job_metrics, "list_run_jobs", side_effect=RuntimeError("503")
        ):
            rows = export_github_job_metrics.collect_rows(
                "ydb-platform", "ydb", "pr_check.yml", datetime.now(timezone.utc), set(), failed
            )
        self.assertEqual(rows, [])
        self.assertEqual(failed, {(5, 1): 1})


class WatermarkDisciplineTest(unittest.TestCase):
    def _patch_main(self, *, collect_raises=None, uploaded=1):
        run = {
            "id": 1,
            "run_attempt": 1,
            "event": "push",
            "name": "PR-check",
            "head_sha": "abc",
            "head_branch": "main",
            "html_url": "https://example.test/run/1",
        }
        rows = metrics_from_workflow_run(run, [
            {
                "id": 11,
                "name": "build",
                "started_at": "2026-09-28T10:00:00Z",
                "completed_at": "2026-09-28T10:05:00Z",
                "conclusion": "success",
                "steps": [],
            }
        ])

        def collect(*_args, **_kwargs):
            if collect_raises:
                raise collect_raises
            return rows

        return patch.multiple(
            export_github_job_metrics,
            workflows_to_export=lambda *a, **k: ["pr_check.yml"],
            load_watermark=lambda *a, **k: (None, True),
            exported_run_ids=lambda *a, **k: set(),
            exported_job_ids=lambda *a, **k: set(),
            load_failed_runs=lambda *a, **k: ({}, True),
            load_open_runs=lambda *a, **k: ([], True),
            iter_workflow_runs=lambda *a, **k: [],
            collect_rows=collect,
            upload_rows=lambda *a, **k: uploaded,
            save_open_runs=lambda *a, **k: True,
            save_failed_runs=lambda *a, **k: True,
        )

    def test_clean_window_advances_the_watermark(self):
        with self._patch_main(), patch.object(
            export_github_job_metrics, "save_watermark", return_value=True
        ) as save:
            self.assertEqual(export_github_job_metrics.main([]), 0)
        save.assert_called_once()

    def test_listing_failure_keeps_the_watermark(self):
        with self._patch_main(collect_raises=RuntimeError("boom")), patch.object(
            export_github_job_metrics, "save_watermark"
        ) as save:
            export_github_job_metrics.main([])
        save.assert_not_called()

    def test_rate_limit_keeps_the_watermark(self):
        with self._patch_main(collect_raises=RateLimitExhausted(0, None)), patch.object(
            export_github_job_metrics, "save_watermark"
        ) as save:
            self.assertEqual(export_github_job_metrics.main([]), 0)
        save.assert_not_called()

    def test_failed_upload_keeps_the_watermark_and_reports(self):
        with self._patch_main(uploaded=0), patch.object(
            export_github_job_metrics, "save_watermark"
        ) as save:
            self.assertEqual(export_github_job_metrics.main([]), 1)
        save.assert_not_called()


class ReadStateTest(unittest.TestCase):
    def test_missing_credentials_are_a_failed_read(self):
        with patch.object(state, "has_send_credentials", return_value=False):
            payload, ok = state.read_state("export_watermark")
        self.assertIsNone(payload)
        self.assertFalse(ok)

    def test_empty_result_is_absent_not_failed(self):
        wrapper = MagicMock()
        wrapper.check_credentials.return_value = True
        wrapper.execute_scan_query.return_value = []
        wrapper.__enter__.return_value = wrapper
        wrapper.__exit__.return_value = False
        with patch.object(state, "has_send_credentials", return_value=True), patch.object(
            state, "_open_ydb_wrapper", return_value=wrapper
        ):
            payload, ok = state.read_state("export_watermark")
        self.assertIsNone(payload)
        self.assertTrue(ok)

    def test_query_error_is_a_failed_read(self):
        wrapper = MagicMock()
        wrapper.check_credentials.return_value = True
        wrapper.execute_scan_query.side_effect = RuntimeError("ydb down")
        wrapper.__enter__.return_value = wrapper
        wrapper.__exit__.return_value = False
        with patch.object(state, "has_send_credentials", return_value=True), patch.object(
            state, "_open_ydb_wrapper", return_value=wrapper
        ):
            payload, ok = state.read_state("export_watermark")
        self.assertIsNone(payload)
        self.assertFalse(ok)


class LoadWatermarkTest(unittest.TestCase):
    def test_payload_returns_exported_until(self):
        with patch.object(
            state,
            "read_state",
            return_value=({"exported_until": "2026-09-01T12:00:00Z"}, True),
        ):
            moment, ok = state.load_watermark()
        self.assertTrue(ok)
        self.assertEqual(moment, datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
