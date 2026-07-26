#!/usr/bin/env python3
"""Unit tests for perf-duty-match parse / merge / suite join."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PTS))

from common.duty_issues import (  # noqa: E402
    attach_tickets_to_report,
    keys_overlap,
    merge_affected,
    parse_match_block,
    render_match_block,
    tickets_for_suite,
    upsert_match_block,
)

SAMPLE = """
#### Важно
1. x

<!-- perf-duty-match
kind: olap
fingerprint: read.cpp:59
keys:
  - read.cpp:59
  - range.Offset <= i.Offset
affected:
  - suite: UploadTpch1000
    db: sas_big_column
    queries: [Query12, Query04]
-->
"""


class ParseTests(unittest.TestCase):
    def test_parse_sample(self):
        b = parse_match_block(SAMPLE)
        self.assertIsNotNone(b)
        self.assertEqual(b["kind"], "olap")
        self.assertEqual(b["fingerprint"], "read.cpp:59")
        self.assertIn("read.cpp:59", b["keys"])
        self.assertEqual(b["affected"][0]["suite"], "UploadTpch1000")
        self.assertEqual(b["affected"][0]["db"], "sas_big_column")
        self.assertEqual(b["affected"][0]["queries"], ["Query12", "Query04"])

    def test_roundtrip_render(self):
        b = parse_match_block(SAMPLE)
        text = render_match_block(
            kind=b["kind"],
            fingerprint=b["fingerprint"],
            keys=b["keys"],
            affected=b["affected"],
        )
        again = parse_match_block(text)
        self.assertEqual(again["keys"], b["keys"])
        self.assertEqual(again["affected"], b["affected"])

    def test_upsert_replaces(self):
        body = "hello\n" + render_match_block(
            kind="olap",
            fingerprint="a",
            keys=["a"],
            affected=[{"suite": "S1", "db": "db1", "queries": ["Q1"]}],
        )
        new = upsert_match_block(
            body,
            {
                "kind": "olap",
                "fingerprint": "b",
                "keys": ["b"],
                "affected": [{"suite": "S2", "db": None, "queries": []}],
            },
        )
        self.assertEqual(new.count("perf-duty-match"), 1)
        p = parse_match_block(new)
        self.assertEqual(p["fingerprint"], "b")
        self.assertEqual(p["affected"][0]["suite"], "S2")


class MergeMatchTests(unittest.TestCase):
    def test_merge_new_suite(self):
        b = parse_match_block(SAMPLE)
        m = merge_affected(
            b, suite="UploadTpch100", db="sas_small_column", queries=["Query03"]
        )
        suites = {a["suite"] for a in m["affected"]}
        self.assertIn("UploadTpch1000", suites)
        self.assertIn("UploadTpch100", suites)

    def test_merge_same_suite_queries(self):
        b = parse_match_block(SAMPLE)
        m = merge_affected(
            b, suite="UploadTpch1000", db="sas_big_column", queries=["Query07"]
        )
        aff = [a for a in m["affected"] if a["suite"] == "UploadTpch1000"][0]
        self.assertIn("Query12", aff["queries"])
        self.assertIn("Query07", aff["queries"])

    def test_keys_overlap(self):
        self.assertTrue(keys_overlap(["read.cpp:59"], ["Read.cpp:59", "other"]))
        self.assertFalse(keys_overlap(["a"], ["b"]))


class JoinTests(unittest.TestCase):
    def test_tickets_for_suite_db(self):
        issues = [
            {
                "number": 1,
                "title": "T1",
                "url": "https://example/1",
                "kind": "olap",
                "fingerprint": "x",
                "keys": ["x"],
                "affected": [
                    {
                        "suite": "UploadTpch1000",
                        "db": "sas_big_column",
                        "queries": ["Query12"],
                    }
                ],
            }
        ]
        hit = tickets_for_suite(
            issues, suite="UploadTpch1000", db="sas_big_column", kind="olap"
        )
        self.assertEqual(len(hit), 1)
        miss = tickets_for_suite(
            issues, suite="UploadTpch1000", db="sas_small_column", kind="olap"
        )
        self.assertEqual(len(miss), 0)

    def test_attach_to_report(self):
        data = {
            "inbox": [
                {"suite": "UploadTpch1000", "db": "sas_big_column", "issue": "failing"},
                {"suite": "Other", "db": "sas_big_column", "issue": "failing"},
            ],
            "ok": [],
        }
        issues = [
            {
                "number": 47871,
                "title": "OLAP: range.Offset",
                "url": "https://github.com/ydb-platform/ydb/issues/47871",
                "kind": "olap",
                "fingerprint": "read.cpp:59",
                "keys": ["read.cpp:59"],
                "affected": [
                    {
                        "suite": "UploadTpch1000",
                        "db": "sas_big_column",
                        "queries": ["Query12"],
                    }
                ],
            }
        ]
        n = attach_tickets_to_report(data, issues, kind="olap")
        self.assertEqual(n, 1)
        self.assertEqual(data["inbox"][0]["tickets"][0]["number"], 47871)
        self.assertEqual(data["inbox"][1]["tickets"], [])
        self.assertEqual(len(data["known_issues"]), 1)

    def test_attach_copies_tickets_onto_finished_twin(self):
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch1000",
                    "db": "sas_big_column",
                    "issue": "in_progress",
                    "finished": {"issue": "failing", "status": "failing"},
                },
            ],
            "ok": [],
        }
        issues = [
            {
                "number": 47871,
                "title": "OLAP: range.Offset",
                "url": "https://github.com/ydb-platform/ydb/issues/47871",
                "kind": "olap",
                "fingerprint": "read.cpp:59",
                "keys": ["read.cpp:59"],
                "affected": [
                    {"suite": "UploadTpch1000", "db": "sas_big_column", "queries": ["Query12"]}
                ],
            }
        ]
        attach_tickets_to_report(data, issues, kind="olap")
        self.assertEqual(data["inbox"][0]["tickets"][0]["number"], 47871)
        self.assertEqual(data["inbox"][0]["finished"]["tickets"][0]["number"], 47871)


if __name__ == "__main__":
    unittest.main()
