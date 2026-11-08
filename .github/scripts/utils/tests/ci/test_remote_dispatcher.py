#!/usr/bin/env python3
"""Resolve seeds, discover_attempts, index-crawl, retry, claim attempt, contracts."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[5]
_DISPATCHER = Path(__file__).resolve().parents[3] / "ci" / "dispatcher"
_GITHUB = _REPO_ROOT / ".github"
sys.path.insert(0, str(_DISPATCHER))

import _common as common  # noqa: E402
from _common import discover_attempts, http_get_bytes_retry  # noqa: E402
import dispatch_remote_executor as dre  # noqa: E402
from ingest_remote_executor_results import (  # noqa: E402
    _is_required_artifact,
    _s3_acl_args,
    _safe_under,
    download_prefix_via_index,
    parse_index_hrefs,
    rewrite_ci_metrics_attribution,
)
from resolve_remote_execution import build_config  # noqa: E402


def patch_env(test: unittest.TestCase, **values: str) -> None:
    patcher = mock.patch.dict(os.environ, values)
    patcher.start()
    test.addCleanup(patcher.stop)


class ResolveSeedsTest(unittest.TestCase):
    def setUp(self):
        patch_env(self, CODE_REF="abcdef1", GITHUB_WORKFLOW="Run and debug tests")
        for key in (
            "REMOTE_JSON",
            "DEFAULT_BUILD_TARGET",
            "DEFAULT_BUILD_PRESET",
            "ANALYTICS_BRANCH",
            "GITHUB_REF_NAME",
        ):
            os.environ.pop(key, None)

    def test_sha_code_ref_maps_analytics_to_main(self):
        self.assertEqual(build_config()["analytics_branch"], "main")

    def test_analytics_branch_env_wins_over_sha_heuristic(self):
        os.environ["ANALYTICS_BRANCH"] = "stable-25-1"
        cfg = build_config()
        self.assertEqual(cfg["analytics_branch"], "stable-25-1")
        self.assertEqual(cfg["inputs"]["job_inputs"]["custom_branch_name"], "stable-25-1")

    def test_remote_code_ref_ignores_analytics_branch_env(self):
        os.environ["ANALYTICS_BRANCH"] = "main"
        os.environ["REMOTE_JSON"] = json.dumps({"code_ref": "stable-25-1"})
        self.assertEqual(build_config()["analytics_branch"], "stable-25-1")

    def test_github_ref_name_not_used_implicitly(self):
        os.environ["GITHUB_REF_NAME"] = "123/merge"
        self.assertEqual(build_config()["analytics_branch"], "main")

    def test_remote_json_analytics_branch_wins_over_env(self):
        os.environ["ANALYTICS_BRANCH"] = "stable-25-1"
        os.environ["REMOTE_JSON"] = json.dumps({"analytics_branch": "custom"})
        self.assertEqual(build_config()["analytics_branch"], "custom")

    def test_ui_seeds_override_defaults_when_remote_omits_job_keys(self):
        os.environ["DEFAULT_BUILD_TARGET"] = "ydb/core/foo"
        os.environ["DEFAULT_BUILD_PRESET"] = "release-asan,relwithdebinfo"
        os.environ["REMOTE_JSON"] = "{}"
        cfg = build_config()
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_target"], "ydb/core/foo")
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_preset"], "release-asan")
        self.assertEqual(cfg["host_s3_endpoint"], "https://s3.us-east-1.amazonaws.com")
        self.assertNotIn("platform_run_id", cfg["inputs"])
        self.assertNotIn("job", cfg)

    def test_remote_json_job_keys_win_over_ui_seeds(self):
        os.environ["DEFAULT_BUILD_TARGET"] = "ydb/from-ui"
        os.environ["DEFAULT_BUILD_PRESET"] = "release-asan"
        os.environ["REMOTE_JSON"] = json.dumps(
            {"job": {"build_target": "ydb/from-remote", "build_preset": "relwithdebinfo"}}
        )
        cfg = build_config()
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_target"], "ydb/from-remote")
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_preset"], "relwithdebinfo")

    def test_unknown_remote_job_key_dies(self):
        os.environ["REMOTE_JSON"] = json.dumps({"job": {"build_tagret": "ydb/x"}})
        with self.assertRaises(SystemExit):
            build_config()

    def test_non_bool_fail_on_remote_failure_dies(self):
        os.environ["REMOTE_JSON"] = json.dumps({"fail_on_remote_failure": "yes"})
        with self.assertRaises(SystemExit):
            build_config()

    def test_non_bool_run_tests_dies(self):
        os.environ["REMOTE_JSON"] = json.dumps({"job": {"run_tests": "yes"}})
        with self.assertRaises(SystemExit):
            build_config()

    def test_non_str_build_preset_dies(self):
        os.environ["REMOTE_JSON"] = json.dumps({"job": {"build_preset": 7}})
        with self.assertRaises(SystemExit):
            build_config()


class IndexCrawlSafetyTest(unittest.TestCase):
    def test_parse_index_rejects_parent_segments(self):
        html = '<a href="../">up</a><a href="a/../../x">bad</a><a href="try_1/report.json">ok</a>'
        self.assertEqual(parse_index_hrefs(html), ["try_1/report.json"])

    def test_safe_under_blocks_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            self.assertIsNone(_safe_under(dest, "a/../../outside"))
            self.assertIsNone(_safe_under(dest, "../x"))
            ok = _safe_under(dest, "try_1/report.json")
            self.assertIsNotNone(ok)
            self.assertTrue(str(ok.resolve()).startswith(str(dest.resolve())))

    def test_discover_attempts_scans_try_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            try_dir = root / "try_1"
            try_dir.mkdir()
            (try_dir / "report.json").write_text("{}", encoding="utf-8")
            (try_dir / "ya-test.html").write_text("<html/>", encoding="utf-8")
            (try_dir / "started_at").write_text("1700000000", encoding="utf-8")
            found = discover_attempts(root)
            self.assertEqual(len(found), 1)
            self.assertEqual(found[0]["retry"], 1)
            self.assertEqual(found[0]["report_relpath"], "try_1/report.json")


class IndexCrawlCompletenessTest(unittest.TestCase):
    def test_is_required_artifact(self):
        self.assertTrue(_is_required_artifact("index.html"))
        self.assertTrue(_is_required_artifact("ci_metrics.jsonl"))
        self.assertTrue(_is_required_artifact("try_1/report.json"))
        self.assertTrue(_is_required_artifact("try_2/ya-test.html"))
        self.assertTrue(_is_required_artifact("try_3/started_at"))
        self.assertTrue(_is_required_artifact("try_1/index.html"))
        self.assertFalse(_is_required_artifact("try_1/logs/a.log"))
        self.assertFalse(_is_required_artifact("summary_text.txt"))

    def _fake_tree(self):
        root_index = (
            '<a href="try_1/index.html">try</a>'
            '<a href="ci_metrics.jsonl">metrics</a>'
        )
        try_index = (
            '<a href="report.json">r</a>'
            '<a href="ya-test.html">h</a>'
            '<a href="started_at">s</a>'
            '<a href="logs/a.log">l</a>'
        )
        return {
            "index.html": root_index.encode(),
            "try_1/index.html": try_index.encode(),
            "try_1/report.json": b"{}",
            "try_1/ya-test.html": b"<html/>",
            "try_1/started_at": b"1700000000",
            "try_1/logs/a.log": b"log",
            "ci_metrics.jsonl": b"{}\n",
        }

    def _fetch_from(self, objects, missing=frozenset()):
        def fetch(url: str, **_kwargs):
            prefix = "https://ep/bucket/prefix/"
            self.assertTrue(url.startswith(prefix), url)
            key = url[len(prefix) :]
            return None if key in missing else objects.get(key)

        return fetch

    def test_all_present_returns_empty_failed(self):
        objects = self._fake_tree()
        with tempfile.TemporaryDirectory() as tmp:
            dest = Path(tmp)
            failed = download_prefix_via_index(
                "https://ep",
                "bucket",
                "prefix",
                dest,
                fetch=self._fetch_from(objects),
            )
            self.assertEqual(failed, [])
            self.assertTrue((dest / "try_1" / "report.json").is_file())
            self.assertTrue((dest / "ci_metrics.jsonl").is_file())
            self.assertTrue((dest / "try_1" / "logs" / "a.log").is_file())

    def test_missing_required_report_dies(self):
        objects = self._fake_tree()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit):
                download_prefix_via_index(
                    "https://ep",
                    "bucket",
                    "prefix",
                    Path(tmp),
                    fetch=self._fetch_from(objects, missing={"try_1/report.json"}),
                )

    def test_missing_try_index_dies(self):
        objects = self._fake_tree()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit):
                download_prefix_via_index(
                    "https://ep",
                    "bucket",
                    "prefix",
                    Path(tmp),
                    fetch=self._fetch_from(objects, missing={"try_1/index.html"}),
                )

    def test_missing_optional_log_warns_only(self):
        objects = self._fake_tree()
        with tempfile.TemporaryDirectory() as tmp:
            failed = download_prefix_via_index(
                "https://ep",
                "bucket",
                "prefix",
                Path(tmp),
                fetch=self._fetch_from(objects, missing={"try_1/logs/a.log"}),
            )
            self.assertEqual(failed, ["try_1/logs/a.log"])
            self.assertTrue((Path(tmp) / "try_1" / "report.json").is_file())


class HttpRetryTest(unittest.TestCase):
    def test_retries_then_succeeds(self):
        calls = {"n": 0}

        class _Resp:
            def read(self):
                return b"ok"

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def fake_urlopen(url, timeout=60):
            calls["n"] += 1
            if calls["n"] < 3:
                raise TimeoutError("boom")
            return _Resp()

        with mock.patch.object(common.urllib.request, "urlopen", fake_urlopen), mock.patch.object(
            common.time, "sleep"
        ) as sleep:
            got = http_get_bytes_retry("https://x", attempts=4, backoff_s=0.01)
        self.assertEqual(got, b"ok")
        self.assertEqual(calls["n"], 3)
        self.assertEqual(sleep.call_count, 2)

    def test_404_not_retried(self):
        calls = {"n": 0}

        def fake_urlopen(url, timeout=60):
            calls["n"] += 1
            raise common.urllib.error.HTTPError(url, 404, "missing", hdrs=None, fp=None)

        with mock.patch.object(common.urllib.request, "urlopen", fake_urlopen), mock.patch.object(
            common.time, "sleep"
        ) as sleep:
            self.assertIsNone(http_get_bytes_retry("https://x", attempts=4, backoff_s=0.01))
        self.assertEqual(calls["n"], 1)
        self.assertEqual(sleep.call_count, 0)

    def test_all_none_returns_none(self):
        def fake_urlopen(url, timeout=60):
            raise TimeoutError("boom")

        with mock.patch.object(common.urllib.request, "urlopen", fake_urlopen), mock.patch.object(
            common.time, "sleep"
        ) as sleep:
            self.assertIsNone(http_get_bytes_retry("https://x", attempts=3, backoff_s=0.01))
        self.assertEqual(sleep.call_count, 2)


class S3AclArgsTest(unittest.TestCase):
    def test_missing_env_defaults_public(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("S3_PUBLIC_ACL_FLAG", None)
            self.assertEqual(_s3_acl_args(), ["--acl-public"])

    def test_explicit_empty_means_no_acl(self):
        with mock.patch.dict(os.environ, {"S3_PUBLIC_ACL_FLAG": ""}, clear=False):
            self.assertEqual(_s3_acl_args(), [])

    def test_explicit_flag_passthrough(self):
        with mock.patch.dict(os.environ, {"S3_PUBLIC_ACL_FLAG": "--acl-public"}, clear=False):
            self.assertEqual(_s3_acl_args(), ["--acl-public"])


class WaitClaimAttemptTest(unittest.TestCase):
    def setUp(self):
        patch_env(
            self,
            PLATFORM_RUN_ID="111",
            PLATFORM_RUN_ATTEMPT="1",
            CONFIG_JSON=json.dumps(
                {
                    "host_s3_endpoint": "https://ep",
                    "host_s3_bucket": "bucket",
                    "repository": "nebius/ydb",
                    "fail_on_remote_failure": True,
                }
            ),
        )

    def test_waits_for_matching_attempt(self):
        os.environ["PLATFORM_RUN_ATTEMPT"] = "2"
        claims = [
            None,
            {
                "platform_run_attempt": "1",
                "executor_run_id": "old",
                "executor_run_url": "https://old",
            },
            {
                "platform_run_attempt": "2",
                "executor_run_id": "99",
                "executor_run_url": "https://run/99",
            },
        ]
        cfg = dre.load_config_json()
        with mock.patch.object(dre, "read_claim", side_effect=claims), mock.patch.object(
            dre.time, "sleep"
        ):
            run_id, run_url = dre.wait_for_claim(cfg)
        self.assertEqual(run_id, "99")
        self.assertEqual(run_url, "https://run/99")

    def test_newer_claim_dies(self):
        cfg = dre.load_config_json()
        claim = {
            "platform_run_attempt": "2",
            "executor_run_id": "99",
            "executor_run_url": "https://run/99",
        }
        with mock.patch.object(dre, "read_claim", return_value=claim), mock.patch.object(
            dre.time, "sleep"
        ):
            with self.assertRaises(SystemExit):
                dre.wait_for_claim(cfg)

    def test_matching_claim_missing_url_dies(self):
        cfg = dre.load_config_json()
        claim = {
            "platform_run_attempt": "1",
            "executor_run_id": "99",
            "executor_run_url": "",
        }
        with mock.patch.object(dre, "read_claim", return_value=claim), mock.patch.object(
            dre.time, "sleep"
        ):
            with self.assertRaises(SystemExit):
                dre.wait_for_claim(cfg)

    def test_claim_timeout_uses_claim_wait_attempts(self):
        os.environ["CLAIM_WAIT_ATTEMPTS"] = "5"
        cfg = dre.load_config_json()
        with mock.patch.object(dre, "read_claim", return_value=None) as read, mock.patch.object(
            dre.time, "sleep"
        ):
            with self.assertRaises(SystemExit):
                dre.wait_for_claim(cfg)
        self.assertEqual(read.call_count, 5)

    def test_claim_timeout_default(self):
        os.environ.pop("CLAIM_WAIT_ATTEMPTS", None)
        cfg = dre.load_config_json()
        with mock.patch.object(dre, "read_claim", return_value=None) as read, mock.patch.object(
            dre.time, "sleep"
        ):
            with self.assertRaises(SystemExit):
                dre.wait_for_claim(cfg)
        self.assertEqual(read.call_count, 120)


class RewriteCiMetricsTest(unittest.TestCase):
    def _rewrite(self, rows, pull_number="7"):
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "ci_metrics.jsonl"
            src.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
            out = rewrite_ci_metrics_attribution(
                src,
                run_id="500",
                event_name="workflow_dispatch",
                workflow="Run and debug tests",
                job_name="Run and debug tests",
                pull_number=pull_number,
                caller_repo="ydb-platform/ydb",
            )
            return [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]

    def test_run_attempt_from_caller_env(self):
        patch_env(self, PLATFORM_RUN_ATTEMPT="3")
        (row,) = self._rewrite([{"run_id": 9, "run_attempt": 1, "labels": {"k": "v"}}])
        self.assertEqual(row["run_id"], 500)
        self.assertEqual(row["run_attempt"], 3)
        self.assertEqual(row["pr_number"], 7)
        self.assertEqual(
            row["labels"],
            {"k": "v", "remote_executor_run_id": 9, "remote_executor_run_attempt": 1},
        )

    def test_bad_pull_number_does_not_crash(self):
        patch_env(self, PLATFORM_RUN_ATTEMPT="1")
        (row,) = self._rewrite([{"run_id": 9}], pull_number="abc")
        self.assertNotIn("pr_number", row)
        self.assertEqual(row["run_attempt"], 1)


class EnqueueOutputTest(unittest.TestCase):
    def test_writes_ticket_and_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            out = tmp_path / "github_output"
            ticket_path = tmp_path / "remote-ticket.json"
            env = {
                "CONFIG_JSON": json.dumps(
                    {
                        "inputs": {"job_inputs": {"build_preset": "relwithdebinfo"}},
                        "host_s3_endpoint": "https://ep",
                        "host_s3_bucket": "b",
                        "repository": "nebius/ydb",
                        "fail_on_remote_failure": True,
                    }
                ),
                "PLATFORM_RUN_ID": "42",
                "PLATFORM_RUN_ATTEMPT": "3",
                "PLATFORM_REPOSITORY": "ydb-platform/ydb",
                "PLATFORM_WORKFLOW": "Run and debug tests",
                "PLATFORM_RUN_URL": "https://github.com/ydb-platform/ydb/actions/runs/42",
                "GITHUB_OUTPUT": str(out),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    dre.cmd_enqueue()
                finally:
                    os.chdir(cwd)
            self.assertTrue(ticket_path.is_file())
            ticket = json.loads(ticket_path.read_text(encoding="utf-8"))
            self.assertEqual(ticket["platform_run_attempt"], "3")
            self.assertIn("ticket_attempt=3", out.read_text(encoding="utf-8"))


class WorkflowContractTest(unittest.TestCase):
    def test_dispatch_remote_pins_ticket_attempt(self):
        text = (_GITHUB / "workflows/dispatch_remote.yml").read_text(encoding="utf-8")
        self.assertIn(
            "PLATFORM_RUN_ATTEMPT: ${{ needs.enqueue.outputs.ticket_attempt }}",
            text,
        )
        self.assertIn("PLATFORM_RUN_ATTEMPT: ${{ github.run_attempt }}", text)

    def test_run_and_debug_passes_analytics_branch(self):
        text = (_GITHUB / "workflows/run_and_debug_tests.yml").read_text(encoding="utf-8")
        self.assertIn("ANALYTICS_BRANCH: ${{ github.ref_name }}", text)


if __name__ == "__main__":
    unittest.main()
