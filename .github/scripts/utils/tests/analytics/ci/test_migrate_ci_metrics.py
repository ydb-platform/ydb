#!/usr/bin/env python3

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics"))

import unittest

from github_actions.migrate_ci_metrics import (
    apply_repairs,
    drop_reason,
    parse_args,
    repair_row,
    row_for_upsert,
)


def _row(**fields):
    base = {
        "date": date(2026, 8, 31),
        "event_ts": datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc),
        "run_id": 10,
        "github_job_id": 20,
        "name": "job",
        "kind": "duration",
        "source": "github_job",
        "event_name": "pull_request_target",
        "commit": "abc",
        "run_attempt": 1,
        "span_id": "job-20",
        "pr_number": None,
        "labels": None,
    }
    base.update(fields)
    return base


class DropReasonTest(unittest.TestCase):
    def test_keeps_a_normal_job(self):
        self.assertIsNone(drop_reason(_row()))

    def test_drops_export_state(self):
        self.assertEqual(drop_reason(_row(source="export_state", run_id=0)), "export_state")

    def test_drops_run_id_zero(self):
        self.assertEqual(drop_reason(_row(run_id=0, source="github_job")), "run_id_zero")

    def test_drops_legacy_ya_names(self):
        self.assertEqual(drop_reason(_row(source="ya_phase", name="ya_rebuild")), "legacy_name")
        self.assertEqual(drop_reason(_row(source="ya_phase", name="tests_total")), "legacy_name")


class RepairRowTest(unittest.TestCase):
    def test_fills_pr_number_from_the_sha_map(self):
        repaired = repair_row(_row(), {"abc": {"number": 54142, "branch": "main"}})
        self.assertEqual(repaired["pr_number"], 54142)
        self.assertEqual(repaired["branch"], "main")

    def test_does_not_overwrite_an_existing_pr_number(self):
        repaired = repair_row(_row(pr_number=1), {"abc": {"number": 9}})
        self.assertEqual(repaired["pr_number"], 1)

    def test_adds_parent_span_id_for_a_phase(self):
        repaired = repair_row(
            _row(source="ya_phase", name="ya_make_try_1", span_id="deadbeef"),
            None,
        )
        labels = json.loads(repaired["labels"])
        self.assertEqual(labels["parent_span_id"], "job-20")

    def test_does_not_set_parent_on_the_job_span_itself(self):
        repaired = repair_row(_row(), None)
        self.assertIsNone(repaired.get("labels"))


class ApplyRepairsTest(unittest.TestCase):
    def test_counts_drops_and_patches(self):
        rows = [
            _row(),
            _row(source="export_state", run_id=0, name="open_runs"),
            _row(source="ya_phase", name="ya_rebuild"),
            _row(source="ya_phase", name="init", span_id="aa", event_name="push"),
        ]
        kept, stats = apply_repairs(rows, {"abc": {"number": 7}})
        self.assertEqual(stats["read"], 4)
        self.assertEqual(stats["dropped"], 2)
        self.assertEqual(stats["kept"], 2)
        self.assertEqual(stats["patched_pr"], 1)
        self.assertEqual(stats["patched_parent"], 1)
        self.assertEqual(kept[0]["pr_number"], 7)
        self.assertEqual(kept[0]["date"], date(2026, 8, 31))


class ParseArgsTest(unittest.TestCase):
    def test_apply_is_accepted_after_the_subcommand(self):
        args = parse_args(["resolve-prs", "--checkpoint", "/tmp/map.json", "--apply"])
        self.assertTrue(args.apply)
        args = parse_args(["copy", "--dest", "analytics/ci_metrics_migration"])
        self.assertFalse(args.apply)


class RowForUpsertTest(unittest.TestCase):
    def test_decodes_ydb_date_days_and_label_bytes(self):
        row = row_for_upsert(
            {
                "date": 20696,
                "event_ts": 1790346591361346,
                "run_id": 1,
                "github_job_id": 2,
                "name": "job",
                "kind": "duration",
                "source": "github_job",
                "run_attempt": 1,
                "span_id": "job-2",
                "labels": b'{"queued_ms":1}',
            }
        )
        self.assertEqual(row["date"], date(2026, 8, 31))
        self.assertEqual(row["event_ts"].year, 2026)
        self.assertEqual(row["labels"], '{"queued_ms":1}')


if __name__ == "__main__":
    unittest.main()
