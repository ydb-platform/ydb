#!/usr/bin/env python3
"""Unit tests for the CI metrics DataLens CLI. No DataLens or YDB required."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

_DATALENS = Path(__file__).resolve().parents[3] / "analytics" / "datalens"
sys.path.insert(0, str(_DATALENS))

import checks
import dl
import rpc
import store


DURATION_SQL = """SELECT
    CAST(t.run_id AS Utf8) AS ci_run_id,
    CAST(t.run_attempt AS Utf8) AS run_attempt
FROM `analytics/ci_metrics` AS t
"""

GANTT_SQL = """
    Unicode::Substring(CAST(s.started_at AS Utf8), 0, 10) || ' ' || Unicode::Substring(CAST(s.started_at AS Utf8), 11, 5) || ' ' || COALESCE(rs.run_icon, '🟡') || ' · ' || COALESCE(Unicode::Substring(t.commit, 0, 7), '') || ' · ' || CAST(t.run_id AS Utf8) || ' · #' || CAST(COALESCE(t.run_attempt, 1UL) AS Utf8) AS run_label
) AS s ON s.run_id = t.run_id AND s.run_attempt = t.run_attempt
) AS rs ON rs.run_id = t.run_id AND rs.run_attempt = t.run_attempt
"""


class SlimDatasetTest(unittest.TestCase):
    def test_replaces_sql_with_placeholder_and_drops_errors(self):
        payload = {
            "dataset": {
                "component_errors": {"items": [1]},
                "sources": [{"parameters": {"subsql": "SELECT 1", "manual": True}}],
                "result_schema": [{"title": "ci_run_id"}],
            }
        }
        slim = store.slim_dataset(payload)
        self.assertNotIn("component_errors", slim)
        self.assertEqual(slim["sources"][0]["parameters"]["subsql"], store.QUERY_PLACEHOLDER)
        injected = store.inject_sql(slim, "SELECT 2")
        self.assertEqual(injected["sources"][0]["parameters"]["subsql"], "SELECT 2")
        self.assertEqual(slim["sources"][0]["parameters"]["subsql"], store.QUERY_PLACEHOLDER)


class ExtractChartTest(unittest.TestCase):
    def test_reads_nested_entry(self):
        payload = {
            "entry": {
                "type": "advanced-chart_node",
                "annotation": {"description": "d"},
                "meta": {},
                "data": {
                    "sources": "src",
                    "prepare": "prep",
                    "params": "p",
                    "controls": "c",
                    "meta": "m",
                },
            }
        }
        extracted = store.extract_chart(payload)
        self.assertEqual(extracted["sources"], "src")
        self.assertEqual(extracted["prepare"], "prep")
        self.assertEqual(extracted["meta"]["data"]["params"], "p")


class RoundTripFilesTest(unittest.TestCase):
    def test_chart_and_dataset_roundtrip(self):
        original = dict(store.OBJECT_DIRS)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            store.OBJECT_DIRS["duration"] = root / "duration"
            store.OBJECT_DIRS["duration-ds"] = root / "duration-ds"
            try:
                self._roundtrip_chart_and_dataset()
            finally:
                store.OBJECT_DIRS.clear()
                store.OBJECT_DIRS.update(original)

    def _roundtrip_chart_and_dataset(self):
        store.write_chart(
            "duration",
            {
                "entry": {
                    "type": "advanced-chart_node",
                    "annotation": {"description": "x"},
                    "data": {"sources": "S", "prepare": "P", "params": "Q", "controls": "{}", "meta": "{}"},
                }
            },
        )
        entry = store.chart_entry("duration", "egg8s3g477lmx")
        self.assertEqual(entry["data"]["sources"], "S\n")
        self.assertEqual(entry["data"]["prepare"], "P\n")
        store.write_dataset(
            "duration-ds",
            {"dataset": {"sources": [{"parameters": {"subsql": "SELECT 1"}}], "result_schema": []}},
        )
        self.assertEqual(store.read_dataset("duration-ds")["sql"], "SELECT 1\n")
        self.assertEqual(
            store.read_dataset("duration-ds")["dataset"]["sources"][0]["parameters"]["subsql"],
            "SELECT 1\n",
        )


class ChecksTest(unittest.TestCase):
    def test_good_texts_pass(self):
        errors = checks.check_texts(
            {
                "duration-ds": {"sql": DURATION_SQL},
                "gantt-ds": {"sql": GANTT_SQL},
                "pickers-ds": {"sql": GANTT_SQL},
                "gantt": {"prepare": "function parseRunRef() {}\nfunction keepOneAttempt() {}\nrun_label"},
                "duration": {"prepare": "row.ci_run_id + row.run_attempt\nrun_label: ['']"},
            }
        )
        self.assertEqual(errors, [])

    def test_rejects_run_id_and_in_progress(self):
        errors = checks.check_texts(
            {
                "duration-ds": {
                    "sql": "SELECT CAST(t.run_id AS Utf8) AS run_id, COALESCE(j.job_conclusion, 'in_progress') FROM t"
                }
            }
        )
        self.assertTrue(any("ci_run_id" in item for item in errors))
        self.assertTrue(any("in_progress" in item for item in errors))

    def test_rejects_short_click_run_label(self):
        errors = checks.check_texts(
            {
                "duration": {
                    "prepare": "row.ci_run_id + row.run_attempt\nrun_label: [String(point.runId || '') + ' · #' + String(point.attempt || '1')]"
                }
            }
        )
        self.assertTrue(any("run_label" in item for item in errors))

    def test_rejects_icon_first_label(self):
        sql = (
            "COALESCE(rs.run_icon, '⚪') || ' ' || Unicode::Substring(CAST(s.started_at AS Utf8), 5, 5) AS run_label\n"
            "ON s.run_id = t.run_id\n"
        )
        errors = checks.check_texts({"gantt-ds": {"sql": sql}})
        self.assertTrue(any("YYYY-MM-DD" in item or "icon" in item or "attempt" in item for item in errors))

    def test_job_status_default_must_be_success_and_failure(self):
        def dash(values):
            return json.dumps({"id": "seljobstatus", "source": {"defaultValue": values}})

        self.assertEqual(checks.check_texts({"dashboard": {"dashboard": dash(["success", "failure"])}}), [])
        errors = checks.check_texts({"dashboard": {"dashboard": dash(["success"])}})
        self.assertTrue(any("success+failure" in item for item in errors))
        errors = checks.check_texts({"dashboard": {"dashboard": dash(["success", "failure", "in_progress"])}})
        self.assertTrue(any("success+failure" in item for item in errors))


class RpcHelpersTest(unittest.TestCase):
    def test_walk_rev_id_prefers_entry(self):
        self.assertEqual(rpc.walk_rev_id({"entry": {"revId": "draft", "publishedId": "old"}}), "draft")
        self.assertEqual(rpc.walk_rev_id({"savedId": "s"}), "s")

    def test_publish_dashboard_uses_draft_revid(self):
        calls = []

        def fake_update(token, org_id, entry, mode, rev_id=None, workbook_id=None, dashboard_id=None):
            calls.append((mode, rev_id))
            if mode == "save":
                return {"entry": {"revId": "draft-1"}}
            return {"entry": {"revId": "pub-1"}}

        with mock.patch.object(rpc, "update_dashboard", side_effect=fake_update):
            result = rpc.publish_dashboard("t", "org", {"entryId": "d"})
        self.assertEqual(result["revId"], "draft-1")
        self.assertEqual(calls, [("save", None), ("publish", "draft-1")])


class CliTest(unittest.TestCase):
    def test_help(self):
        with self.assertRaises(SystemExit) as ctx:
            dl.main(["--help"])
        self.assertEqual(ctx.exception.code, 0)

    def test_publish_without_apply_does_not_call_api(self):
        with mock.patch.object(dl, "check_texts", return_value=[]), mock.patch.object(
            dl, "iam_token", return_value="t"
        ) as token, mock.patch.object(dl, "publish_editor_chart") as pub, mock.patch.object(
            dl, "chart_entry", return_value={"entryId": "x"}
        ):
            code = dl.main(["publish", "duration"])
        self.assertEqual(code, 0)
        pub.assert_not_called()
        token.assert_not_called()

    def test_local_objects_pass_check_when_present(self):
        dest = store.OBJECT_DIRS["duration-ds"] / "query.sql"
        if not dest.is_file():
            self.skipTest("objects/ not pulled")
        self.assertEqual(checks.check_texts(), [])


if __name__ == "__main__":
    unittest.main()
