#!/usr/bin/env python3
"""Collector: no GitHub / CI context required."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from collector import (
    end,
    enrich,
    flush_file,
    main,
    normalize_metric,
    send,
    start,
    track,
)
from collector.buffer import append_record, read_pending_spans, write_send_offset
from collector.spans import attach_context
from collector.values import build_track_record, parse_datetime, parse_labels


class CollectorNormalizeTest(unittest.TestCase):
    def test_generic_row_has_no_github_columns(self):
        row = normalize_metric(
            {
                "name": "my_step",
                "kind": "info",
                "source": "my_job",
                "run_id": 42,
                "span_id": "span-1",
                "event_ts": "2026-09-21T10:00:00Z",
                "labels": {"model": "foo", "tokens": 12},
            }
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["name"], "my_step")
        self.assertEqual(row["source"], "my_job")
        self.assertEqual(row["run_id"], 42)
        self.assertNotIn("github_job_id", row)
        self.assertNotIn("workflow", row)
        self.assertEqual(json.loads(row["labels"])["tokens"], 12)

    def test_skips_missing_source_or_timestamp(self):
        self.assertIsNone(normalize_metric({"name": "my_step", "run_id": 1, "event_ts": "2026-09-21T10:00:00Z"}))
        self.assertIsNone(normalize_metric({"name": "my_step", "run_id": 1, "source": "my_job"}))

    def test_skips_missing_span_id(self):
        self.assertIsNone(
            normalize_metric(
                {
                    "name": "my_step",
                    "run_id": 1,
                    "source": "my_job",
                    "event_ts": "2026-09-21T10:00:00Z",
                }
            )
        )


class CollectorLifecycleTest(unittest.TestCase):
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
        record = attach_context({"name": "my_step"})
        self.assertEqual(record["run_id"], 99)
        self.assertNotIn("workflow", record)
        labels = record.get("labels") or {}
        self.assertNotIn("github.sha", labels)

    def test_start_send_duration_and_info_snapshot(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None, **kwargs):
            sends.append(path)
            return 0

        import collector.flush as flush_mod

        original = flush_mod.flush_file
        flush_mod.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                self.assertEqual(
                    main(
                        [
                            "start",
                            "my_step",
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
                self.assertEqual(names["my_step"]["kind"], "duration")
                self.assertEqual(names["my_step"]["value"], 3000.0)
                self.assertEqual(names["my_step"]["source"], "arcadia")
                self.assertEqual(names["my_step"]["labels"]["tokens"], "12")
                self.assertEqual(sends, [path])
        finally:
            flush_mod.flush_file = original

    def test_track_sets_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            track("my_step", {"value": 1, "kind": "count", "tokens": 3}, file=path, source="llm_eval")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["name"], "my_step")
            self.assertEqual(row["source"], "llm_eval")
            self.assertEqual(row["labels"]["tokens"], 3)
            self.assertNotIn("github.sha", row["labels"])

    def test_track_without_github_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            track("my_step", {"model": "foo"}, file=path, source="arcadia", kind="event")
            with open(path, encoding="utf-8") as handle:
                row = json.loads(handle.readline())
            self.assertEqual(row["kind"], "event")
            self.assertEqual(row["labels"]["model"], "foo")
            self.assertEqual(row["run_id"], 99)

    def test_ignores_github_run_id(self):
        os.environ.pop("ANALYTICS_RUN_ID", None)
        os.environ["GITHUB_RUN_ID"] = "123"
        record = attach_context({"name": "my_step"})
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
            enrich(
                "ya_make_try_1",
                {"match_labels": {"ya_attempt": "1"}, "report_url": "try1"},
                file=path,
            )
            with open(path, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            by_name = {row["name"]: row for row in rows}
            self.assertEqual(by_name["ya_make_try_1"]["labels"]["report_url"], "try1")
            self.assertNotIn("report_url", by_name["ya_make_try_2"]["labels"])

    def test_send_without_open_span_does_not_invent_track(self):
        sends = []

        def fake_flush(path=None, table_path=None, defaults=None, **kwargs):
            sends.append(path)
            return 0

        import collector.flush as flush_mod

        original = flush_mod.flush_file
        flush_mod.flush_file = fake_flush
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                self.assertEqual(send("ghost", file=path, conclusion="cancelled"), 0)
                self.assertFalse(os.path.exists(path))
                self.assertEqual(sends, [path])
        finally:
            flush_mod.flush_file = original

    def test_start_without_name_returns_error(self):
        self.assertEqual(main(["start"]), 1)
        self.assertEqual(main(["track"]), 1)
        self.assertEqual(main(["enrich"]), 1)

    def test_enrich_via_functions(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "analytics.jsonl")
            start("my_step", file=path, source="llm_eval", started_epoch="1000")
            self.assertEqual(end("my_step", file=path, conclusion="success", finished_epoch="1002"), 1)
            self.assertEqual(enrich("my_step", {"report_url": "https://s3.example/x"}, file=path), 1)
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

        import collector.flush as flush_mod

        original = flush_mod.upsert_metrics
        saved_cred = os.environ.get("ANALYTICS_YDB_CREDENTIALS")
        flush_mod.upsert_metrics = lambda wrapper, rows, **kwargs: len(rows)
        os.environ["ANALYTICS_YDB_CREDENTIALS"] = "1"
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                track("my_step", {"value": 1, "kind": "count"}, file=path, source="arcadia")
                uploaded = flush_file(path, ydb_wrapper_factory=factory)
            self.assertEqual(uploaded, 1)
            self.assertEqual(len(created), 1)
        finally:
            flush_mod.upsert_metrics = original
            if saved_cred is None:
                os.environ.pop("ANALYTICS_YDB_CREDENTIALS", None)
            else:
                os.environ["ANALYTICS_YDB_CREDENTIALS"] = saved_cred

    def test_flush_error_uses_sys_stderr(self):
        import collector.flush as flush_mod

        original = flush_mod.upsert_metrics
        saved = os.environ.get("ANALYTICS_YDB_CREDENTIALS")
        os.environ["ANALYTICS_YDB_CREDENTIALS"] = "1"

        def boom(*args, **kwargs):
            raise RuntimeError("ydb down")

        class Wrapper:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def check_credentials(self):
                return True

            def get_table_path(self, key):
                raise KeyError(key)

        flush_mod.upsert_metrics = boom
        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                track("my_step", {"value": 1, "kind": "count"}, file=path, source="arcadia")
                buf = io.StringIO()
                err = sys.stderr
                sys.stderr = buf
                try:
                    uploaded = flush_file(path, ydb_wrapper_factory=lambda: Wrapper)
                finally:
                    sys.stderr = err
            self.assertEqual(uploaded, 0)
            self.assertIn("ydb down", buf.getvalue())
            self.assertNotIn("NameError", buf.getvalue())
        finally:
            flush_mod.upsert_metrics = original
            if saved is None:
                os.environ.pop("ANALYTICS_YDB_CREDENTIALS", None)
            else:
                os.environ["ANALYTICS_YDB_CREDENTIALS"] = saved

    def test_mixed_batch_writes_skipped_file(self):
        import collector.flush as flush_mod

        original = flush_mod.upsert_metrics
        saved = os.environ.get("ANALYTICS_YDB_CREDENTIALS")
        os.environ["ANALYTICS_YDB_CREDENTIALS"] = "1"
        flush_mod.upsert_metrics = lambda wrapper, rows, **kwargs: len(rows)

        class Wrapper:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def check_credentials(self):
                return True

            def get_table_path(self, key):
                raise KeyError(key)

        try:
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "analytics.jsonl")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("not-json\n")
                    handle.write(
                        json.dumps(
                            {
                                "name": "ok",
                                "source": "arcadia",
                                "run_id": 1,
                                "span_id": "span-ok",
                                "event_ts": "2026-09-21T10:00:00Z",
                                "value": 1,
                            }
                        )
                        + "\n"
                    )
                uploaded = flush_file(path, ydb_wrapper_factory=lambda: Wrapper)
                self.assertEqual(uploaded, 1)
                skipped = Path(path + ".skipped").read_text(encoding="utf-8")
                self.assertIn("invalid json", skipped)
        finally:
            flush_mod.upsert_metrics = original
            if saved is None:
                os.environ.pop("ANALYTICS_YDB_CREDENTIALS", None)
            else:
                os.environ["ANALYTICS_YDB_CREDENTIALS"] = saved


class CollectorValuesTest(unittest.TestCase):
    def test_epoch_strings_and_iso(self):
        self.assertEqual(parse_datetime("1000"), datetime.fromtimestamp(1000, tz=timezone.utc))
        self.assertEqual(
            parse_datetime("2026-09-21T10:00:00Z"),
            datetime(2026, 9, 21, 10, 0, tzinfo=timezone.utc),
        )
        usec = 1_758_792_003_000_000  # YDB Timestamp, microseconds
        self.assertEqual(parse_datetime(usec), datetime.fromtimestamp(usec / 1_000_000, tz=timezone.utc))
        self.assertEqual(parse_datetime(usec // 1000), datetime.fromtimestamp(usec / 1_000_000, tz=timezone.utc))

    def test_parse_labels(self):
        self.assertEqual(parse_labels(["cache_mode=none", "ya_attempt=2"])["cache_mode"], "none")
        self.assertEqual(parse_labels(["bad"]), {})

    def test_build_record_duration_from_epochs(self):
        record = build_track_record(
            "ya_make_try_1",
            {
                "source": "ya_phase",
                "started_epoch": "1000",
                "finished_epoch": "1010.5",
                "conclusion": "success",
                "ya_attempt": 1,
            },
        )
        self.assertEqual(record["value"], 10500.0)
        self.assertEqual(record["labels"]["ya_attempt"], 1)

    def test_append_record_creates_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "out.jsonl")
            append_record(path, {"name": "x"})
            self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
