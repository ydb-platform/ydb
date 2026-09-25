#!/usr/bin/env python3
"""Core analytics client: no GitHub / CI context required."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _paths import ANALYTICS, add_product_paths

add_product_paths(ANALYTICS)

import json
import os
import tempfile
import unittest

from core import (
    attach_context,
    end,
    enrich,
    flush_file,
    main,
    normalize_metric,
    read_pending_spans,
    start,
    track,
    write_send_offset,
)


class CoreNormalizeTest(unittest.TestCase):
    def test_generic_row_has_no_github_columns(self):
        row = normalize_metric(
            {
                "name": "llm_call",
                "kind": "info",
                "source": "arcadia",
                "run_id": 42,
                "event_ts": "2026-09-21T10:00:00Z",
                "labels": {"model": "foo", "tokens": 12},
            }
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["name"], "llm_call")
        self.assertEqual(row["source"], "arcadia")
        self.assertEqual(row["run_id"], 42)
        self.assertNotIn("github_job_id", row)
        self.assertNotIn("workflow", row)
        self.assertEqual(json.loads(row["labels"])["tokens"], 12)


class CoreLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.saved = {key: os.environ.get(key) for key in ("ANALYTICS_RUN_ID", "GITHUB_RUN_ID", "GITHUB_WORKFLOW")}
        os.environ.pop("GITHUB_RUN_ID", None)
        os.environ.pop("GITHUB_WORKFLOW", None)
        os.environ["ANALYTICS_RUN_ID"] = "99"

    def tearDown(self):
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_attach_context_is_not_github(self):
        record = attach_context({"name": "llm_call"})
        self.assertEqual(record["run_id"], 99)
        self.assertNotIn("workflow", record)
        labels = record.get("labels") or {}
        self.assertNotIn("github.sha", labels)

    def test_start_send_duration_and_info_snapshot(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None, **kwargs):
            sends.append(path)
            return 0

        import core as client

        original = client.flush_file
        client.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                self.assertEqual(
                    main(
                        [
                            "start",
                            "llm_call",
                            "--file",
                            path,
                            "--source",
                            "arcadia",
                            "--started-epoch",
                            "1000",
                            "--run-id",
                            "7",
                            "--label",
                            "model=foo",
                        ]
                    ),
                    0,
                )
                pending = read_pending_spans(path)
                self.assertEqual(pending[0]["run_id"], 7)
                self.assertNotIn("github.workflow", (pending[0].get("labels") or {}))
                self.assertEqual(
                    main(
                        [
                            "send",
                            "--file",
                            path,
                            "--conclusion",
                            "success",
                            "--finished-epoch",
                            "1003",
                            "--label",
                            "tokens=12",
                        ]
                    ),
                    0,
                )
                with open(path, encoding="utf-8") as handle:
                    rows = [json.loads(line) for line in handle if line.strip()]
                names = {row["name"]: row for row in rows}
                self.assertEqual(names["llm_call"]["kind"], "duration")
                self.assertEqual(names["llm_call"]["value"], 3000.0)
                self.assertEqual(names["llm_call"]["source"], "arcadia")
                self.assertEqual(names["llm_call"]["labels"]["tokens"], "12")
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_track_sets_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            track("llm_call", {"value": 1, "kind": "count", "tokens": 3}, file=path, source="llm_eval")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], "llm_call")
            self.assertEqual(row["source"], "llm_eval")
            self.assertEqual(row["labels"]["tokens"], 3)
            self.assertNotIn("github.sha", row["labels"])

    def test_track_without_github_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            track("llm_call", {"model": "foo"}, file=path, source="arcadia", kind="event")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["kind"], "event")
            self.assertEqual(row["labels"]["model"], "foo")
            self.assertEqual(row["run_id"], 99)

    def test_ignores_github_run_id(self):
        os.environ.pop("ANALYTICS_RUN_ID", None)
        os.environ["GITHUB_RUN_ID"] = "123"
        record = attach_context({"name": "llm_call"})
        self.assertNotIn("run_id", record)

    def test_enrich_adds_url_without_changing_duration(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            start("dashboard", file=path, source="ya_phase", started_epoch="1000")
            self.assertEqual(end("dashboard", file=path, conclusion="success", finished_epoch="1002"), 1)
            self.assertEqual(
                enrich(
                    "dashboard",
                    {"report_url": "https://s3.example/dashboard.html", "error": "late"},
                    file=path,
                ),
                1,
            )
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["value"], 2000.0)
            self.assertEqual(row["conclusion"], "success")
            self.assertEqual(row["event_ts"], "1970-01-01T00:16:40.000000Z")
            self.assertEqual(row["labels"]["report_url"], "https://s3.example/dashboard.html")
            self.assertEqual(row["labels"]["error"], "late")

    def test_enrich_matches_ya_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            start("ya_make_try_1", {"ya_attempt": "1"}, file=path, source="ya_phase")
            end("ya_make_try_1", file=path, conclusion="success")
            start("ya_make_try_2", {"ya_attempt": "2"}, file=path, source="ya_phase")
            end("ya_make_try_2", file=path, conclusion="failure")
            enrich("ya_make_try_1", {"ya_attempt": "1", "report_url": "try1"}, file=path)
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            by_name = {row["name"]: row for row in rows}
            self.assertEqual(by_name["ya_make_try_1"]["labels"]["report_url"], "try1")
            self.assertNotIn("report_url", by_name["ya_make_try_2"]["labels"])

    def test_start_without_name_returns_error(self):
        self.assertEqual(main(["start"]), 1)
        self.assertEqual(main(["track"]), 1)
        self.assertEqual(main(["enrich"]), 1)

    def test_enrich_via_functions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            start("llm_call", file=path, source="llm_eval", started_epoch="1000")
            self.assertEqual(end("llm_call", file=path, conclusion="success", finished_epoch="1002"), 1)
            self.assertEqual(enrich("llm_call", {"report_url": "https://s3.example/x"}, file=path), 1)
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["value"], 2000.0)
            self.assertEqual(row["labels"]["report_url"], "https://s3.example/x")

    def test_enrich_after_non_ascii_sent_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            start("dashboard", file=path, source="ya_phase", started_epoch="1000")
            end("dashboard", file=path, conclusion="success", finished_epoch="1002")
            with open(path, encoding="utf-8") as handle:
                rest = handle.read()
            sent = json.dumps({"name": "sent", "labels": {"msg": "ошибка"}}, ensure_ascii=False) + "\n"
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(sent + rest)
            write_send_offset(path, len(sent.encode("utf-8")))
            self.assertEqual(enrich("dashboard", {"report_url": "https://s3.example/d.html"}, file=path), 1)
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual(rows[0]["labels"]["msg"], "ошибка")
            self.assertEqual(rows[1]["labels"]["report_url"], "https://s3.example/d.html")

    def test_flush_instantiates_wrapper_class(self):
        created = []

        class Wrapper:
            def __init__(self):
                created.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def check_credentials(self):
                return True

            def get_table_path(self, key):
                raise KeyError(key)

        def factory():
            return Wrapper

        import core as client

        original = client.upsert_metrics
        saved_cred = os.environ.get("ANALYTICS_YDB_CREDENTIALS")
        client.upsert_metrics = lambda wrapper, rows, **kwargs: len(rows)
        os.environ["ANALYTICS_YDB_CREDENTIALS"] = "1"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                track("llm_call", {"value": 1, "kind": "count"}, file=path, source="arcadia")
                uploaded = flush_file(path, ydb_wrapper_factory=factory)
            self.assertEqual(uploaded, 1)
            self.assertEqual(len(created), 1)
        finally:
            client.upsert_metrics = original
            if saved_cred is None:
                os.environ.pop("ANALYTICS_YDB_CREDENTIALS", None)
            else:
                os.environ["ANALYTICS_YDB_CREDENTIALS"] = saved_cred


if __name__ == "__main__":
    unittest.main()
