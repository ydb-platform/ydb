#!/usr/bin/env python3
"""Unit tests for the CI metrics DataLens CLI. No DataLens or YDB required."""

from __future__ import annotations

import argparse
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


class IntervalAndFingerprintTest(unittest.TestCase):
    def test_split_interval_keeps_relative_tokens(self):
        self.assertEqual(
            checks.split_interval("__interval___relative_-4d___relative_+0d"),
            ("__relative_-4d", "__relative_+0d"),
        )
        self.assertIsNone(checks.split_interval(""))

    def test_live_fingerprint_adds_trailing_newline(self):
        live = store.live_fingerprint(
            "chart",
            {"entry": {"data": {"sources": "S", "prepare": "P", "params": "", "controls": ""}}},
        )
        self.assertEqual(live["sources"], "S\n")
        self.assertEqual(live["prepare"], "P\n")
        sql = store.live_fingerprint(
            "dataset",
            {"dataset": {"sources": [{"parameters": {"subsql": "SELECT 1"}}], "result_schema": []}},
        )
        self.assertEqual(sql["sql"], "SELECT 1\n")


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

    def test_rejects_datalens_html_attrs(self):
        errors = checks.check_texts({"capacity-demand": {"prepare": '<div data-wf="PR-check">'}})
        self.assertTrue(any("data-wf" in item for item in errors))

    def test_rejects_lexical_job_conclusion_and_narrow_steps(self):
        errors = checks.check_texts(
            {
                "duration-ds": {
                    "sql": (
                        "SELECT CAST(t.run_id AS Utf8) AS ci_run_id, CAST(t.run_attempt AS Utf8) AS run_attempt, "
                        "MAX(conclusion) AS job_conclusion FROM t "
                        "WHERE t.source = 'github_step' AND t.name IN ('Checkout', 'Build and test')"
                    )
                }
            }
        )
        self.assertTrue(any("lexical" in item for item in errors))
        self.assertTrue(any("github_step" in item for item in errors))

    def test_rejects_pdiff_without_attempt(self):
        errors = checks.check_texts(
            {
                "pdiff": {
                    "prepare": "row.run_id + getHours()",
                    "sources": "columns: ['ci_run_id']\n" + checks.INTERVAL_SPLIT + "\nGTE",
                }
            }
        )
        self.assertTrue(any("ci_run_id" in item for item in errors))
        self.assertTrue(any("run_attempt" in item for item in errors))

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
        self.assertIsNone(rpc.walk_rev_id({"entry": {"id": "chart-entry-id", "publishedId": "old"}}))

    def test_publish_dashboard_uses_draft_revid(self):
        calls = []

        def fake_update(token, org_id, entry, mode, rev_id=None, workbook_id=None, dashboard_id=None):
            calls.append((mode, rev_id, workbook_id, dashboard_id))
            if mode == "save":
                return {"entry": {"revId": "draft-1"}}
            return {"entry": {"revId": "pub-1"}}

        with mock.patch.object(rpc, "update_dashboard", side_effect=fake_update):
            result = rpc.publish_dashboard("t", "org", {"entryId": "d"}, workbook_id="wb", dashboard_id="dash")
        self.assertEqual(result["revId"], "draft-1")
        self.assertEqual(calls, [("save", None, "wb", "dash"), ("publish", "draft-1", "wb", "dash")])

    def test_update_chart_puts_revid_on_entry(self):
        bodies = []

        def fake_rpc(method, body, token, org_id, timeout=120):
            bodies.append(body)
            return {}

        with mock.patch.object(rpc, "rpc", side_effect=fake_rpc):
            rpc.update_editor_chart("t", "org", {"entryId": "c"}, "publish", rev_id="draft-2", workbook_id="wb")
            rpc.update_dashboard("t", "org", {"entryId": "d"}, "publish", rev_id="draft-3", workbook_id="wb", dashboard_id="dash")
        self.assertEqual(bodies[0]["entry"]["revId"], "draft-2")
        self.assertNotIn("revId", bodies[0])
        self.assertEqual(bodies[0]["workbookId"], "wb")
        self.assertEqual(bodies[1]["entry"]["revId"], "draft-3")
        self.assertEqual(bodies[1]["workbookId"], "wb")
        self.assertEqual(bodies[1]["dashboardId"], "dash")

    def test_dataset_and_entry_ids(self):
        self.assertEqual(rpc.dataset_id_from({"id": "ds1"}), "ds1")
        self.assertEqual(rpc.dataset_id_from({"dataset": {"id": "ds2"}}), "ds2")
        self.assertEqual(rpc.entry_id_from({"entry": {"entryId": "ch1"}}), "ch1")
        self.assertEqual(rpc.dataset_from_validate({"data": {"dataset": {"sources": []}}})["sources"], [])


class CreateHelpersTest(unittest.TestCase):
    def test_subselect_dataset_keeps_sql_and_connection(self):
        dataset = store.ydb_subselect_dataset("CI capacity", "SELECT 1", "conn", "desc")
        self.assertEqual(dataset["sources"][0]["parameters"]["subsql"], "SELECT 1")
        self.assertEqual(dataset["sources"][0]["connection_id"], "conn")
        self.assertEqual(dataset["sources"][0]["source_type"], "YDB_SUBSELECT")
        self.assertEqual(dataset["source_avatars"][0]["source_id"], dataset["sources"][0]["id"])

    def test_tab_replaces_same_id_and_shows_tabs(self):
        entry = {"data": {"tabs": [{"id": "old"}, {"id": "captab01", "title": "stale"}], "settings": {"hideTabs": True}}}
        tab = store.dashboard_tab("captab01", "Capacity", [{
            "widget_id": "captight01",
            "title": "Quota",
            "chart_id": "abc",
            "height": 12,
        }])
        store.upsert_dashboard_tab(entry, tab)
        self.assertIs(entry["data"]["settings"]["hideTabs"], False)
        self.assertEqual([item["id"] for item in entry["data"]["tabs"]], ["old", "captab01"])
        self.assertEqual(entry["data"]["tabs"][1]["items"][0]["data"]["tabs"][0]["chartId"], "abc")

    def test_create_dataset_dry_run_does_not_call_api(self):
        args = argparse.Namespace(
            create_kind="dataset",
            name="capacity-ds",
            title="CI capacity",
            description="",
            apply=False,
        )
        manifest = {"objects": {}, "org_id": "o", "workbook_id": "w"}
        with mock.patch.object(dl, "iam_token") as token, mock.patch.object(dl, "create_dataset") as create:
            code = dl.cmd_create(args, manifest)
        self.assertEqual(code, 0)
        token.assert_not_called()
        create.assert_not_called()
        self.assertNotIn("capacity-ds", manifest["objects"])


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
