#!/usr/bin/env python3
"""Core analytics client: no GitHub / CI context required."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from core import (
    INFO_SNAPSHOT_NAME,
    Analytics,
    attach_context,
    main,
    normalize_metric,
    read_pending_spans,
    track,
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
        self.assertNotIn("cicd.pipeline.name", labels)

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
                            "--attr",
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
                            "--json",
                            json.dumps({"payload": {"tokens": 12, "prompt": "hi"}}),
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
                self.assertEqual(names[INFO_SNAPSHOT_NAME]["kind"], "info")
                self.assertEqual(names[INFO_SNAPSHOT_NAME]["labels"]["payload"]["tokens"], 12)
                self.assertEqual(sends, [path])
        finally:
            client.flush_file = original

    def test_analytics_client_default_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            analytics = Analytics(file=path, source="llm_eval")
            analytics.track("llm_call", {"value": 1, "kind": "count", "tokens": 3})
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


if __name__ == "__main__":
    unittest.main()
