#!/usr/bin/env python3
"""Pure-function checks for remote resolve seeds and index crawl path safety."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_DISPATCHER = Path(__file__).resolve().parents[3] / "ci" / "dispatcher"
sys.path.insert(0, str(_DISPATCHER))

from _common import discover_attempts  # noqa: E402
from ingest_remote_executor_results import _safe_under, parse_index_hrefs  # noqa: E402
from resolve_remote_execution import build_config  # noqa: E402


class ResolveSeedsTest(unittest.TestCase):
    def setUp(self):
        self._env = {
            k: os.environ.get(k)
            for k in (
                "REMOTE_JSON",
                "DEFAULT_BUILD_TARGET",
                "DEFAULT_BUILD_PRESET",
                "CODE_REF",
                "GITHUB_WORKFLOW",
            )
        }
        os.environ["CODE_REF"] = "abcdef1"
        os.environ["GITHUB_WORKFLOW"] = "Run and debug tests"
        os.environ.pop("REMOTE_JSON", None)
        os.environ.pop("DEFAULT_BUILD_TARGET", None)
        os.environ.pop("DEFAULT_BUILD_PRESET", None)

    def tearDown(self):
        for key, value in self._env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_ui_seeds_override_defaults_when_remote_omits_job_keys(self):
        os.environ["DEFAULT_BUILD_TARGET"] = "ydb/core/foo"
        os.environ["DEFAULT_BUILD_PRESET"] = "release-asan,relwithdebinfo"
        os.environ["REMOTE_JSON"] = "{}"
        cfg = build_config()
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_target"], "ydb/core/foo")
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_preset"], "release-asan")
        self.assertEqual(cfg["host_s3_endpoint"], "https://s3.us-east-1.amazonaws.com")
        self.assertNotIn("platform_run_id", cfg["inputs"])

    def test_remote_json_job_keys_win_over_ui_seeds(self):
        os.environ["DEFAULT_BUILD_TARGET"] = "ydb/from-ui"
        os.environ["DEFAULT_BUILD_PRESET"] = "release-asan"
        os.environ["REMOTE_JSON"] = json.dumps(
            {"job": {"build_target": "ydb/from-remote", "build_preset": "relwithdebinfo"}}
        )
        cfg = build_config()
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_target"], "ydb/from-remote")
        self.assertEqual(cfg["inputs"]["job_inputs"]["build_preset"], "relwithdebinfo")


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


if __name__ == "__main__":
    unittest.main()
