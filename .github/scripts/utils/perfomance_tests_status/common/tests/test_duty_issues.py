#!/usr/bin/env python3
"""Unit tests for perf-duty-match parse / merge / suite join."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PTS = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PTS))

from datetime import datetime, timedelta, timezone  # noqa: E402

from common.duty_decisions import (  # noqa: E402
    attach_duty_decisions_to_report,
    empty_index,
    focus_key,
    merge_decision_into_index,
)
from common.duty_issues import (  # noqa: E402
    CLOSED_ISSUES_MAX_AGE_DAYS,
    affected_would_expand,
    aggregate_run_coverage,
    attach_tickets_to_report,
    branch_label_match,
    classify_fail_coverage,
    closed_issues_since_date,
    is_recently_closed,
    keys_overlap,
    merge_affected,
    norm_branch_label,
    norm_query_name,
    parse_github_ts,
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

    def test_affected_would_expand_skips_same_coverage(self):
        b = parse_match_block(SAMPLE)
        # Same suite@db@query already in block (open_ticket re-annotate).
        self.assertFalse(
            affected_would_expand(
                b,
                suite="UploadTpch1000",
                db="sas_big_column",
                queries=["Query12"],
            )
        )
        # New query on same suite → expand.
        self.assertTrue(
            affected_would_expand(
                b,
                suite="UploadTpch1000",
                db="sas_big_column",
                queries=["Query99"],
            )
        )
        # New suite → expand.
        self.assertTrue(
            affected_would_expand(
                b,
                suite="UploadTpch100",
                db="sas_small_column",
                queries=["Query05"],
            )
        )
        # Empty block → expand.
        self.assertTrue(
            affected_would_expand(
                {"affected": []},
                suite="UploadTpch1000",
                db="sas_small_column",
                queries=["Query05"],
            )
        )

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

    def test_closed_ticket_shown_and_covers(self):
        from common.duty_issues import classify_fail_coverage

        issues = [
            {
                "number": 99,
                "title": "old crash",
                "url": "https://example/99",
                "kind": "olap",
                "state": "closed",
                "closed_at": "2026-08-05T12:00:00Z",
                "fingerprint": "x",
                "keys": ["x"],
                "labels": ["main"],
                "affected": [
                    {
                        "suite": "UploadTpch100",
                        "db": "sas_small_column",
                        "queries": ["Query06"],
                    }
                ],
            }
        ]
        cov = classify_fail_coverage(
            issues,
            suite="UploadTpch100",
            db="sas_small_column",
            branch="main",
            query="Query06",
            kind="olap",
        )
        self.assertEqual(cov["status"], "covered")
        self.assertEqual(cov["tickets"][0]["state"], "closed")
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch100",
                    "db": "sas_small_column",
                    "branch": "main",
                    "issue": "failing",
                    "queries": [{"test": "Query06", "kind": "fail"}],
                }
            ],
            "ok": [],
        }
        attach_tickets_to_report(data, issues, kind="olap")
        self.assertEqual(data["known_issues"][0]["state"], "closed")
        self.assertEqual(data["inbox"][0]["tickets"][0]["state"], "closed")

    def test_closed_ticket_after_sha_is_new_issue(self):
        """Fail on a run/SHA after closed_at → uncovered + new_issue (post_close pill)."""
        from datetime import datetime, timezone

        from common.duty_issues import classify_fail_coverage, run_as_of

        issues = [
            {
                "number": 47284,
                "title": "CountersForStep",
                "url": "https://example/47284",
                "kind": "olap",
                "state": "closed",
                "closed_at": "2026-07-31T11:28:09Z",
                "fingerprint": "CountersForStep",
                "keys": ["CountersForStep"],
                "labels": ["main"],
                "affected": [
                    {
                        "suite": "UploadTpch1000",
                        "db": "vla_small_column",
                        "queries": ["Query01"],
                    }
                ],
            }
        ]
        as_of = datetime(2026, 8, 11, 18, 41, 7, tzinfo=timezone.utc)
        cov = classify_fail_coverage(
            issues,
            suite="UploadTpch1000",
            db="vla_small_column",
            branch="main",
            query="Query01",
            kind="olap",
            as_of=as_of,
        )
        self.assertEqual(cov["status"], "uncovered")
        self.assertTrue(cov["tickets"][0].get("post_close"))
        # Still covered when tested point is on/before close.
        cov_old = classify_fail_coverage(
            issues,
            suite="UploadTpch1000",
            db="vla_small_column",
            branch="main",
            query="Query01",
            kind="olap",
            as_of=datetime(2026, 7, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(cov_old["status"], "covered")

        data = {
            "inbox": [
                {
                    "suite": "UploadTpch1000",
                    "db": "vla_small_column",
                    "branch": "main",
                    "issue": "failing",
                    "queries": [{"test": "Query01", "kind": "fail"}],
                    "now_runs": [
                        {
                            "label": "2026-08-11_6c40390",
                            "version": "6c40390",
                            "ts": "2026-08-11T18:41:07",
                            "day": "2026-08-11",
                            "fail": 1,
                            "fail_tests": "Query01",
                        }
                    ],
                }
            ],
            "ok": [],
        }
        self.assertEqual(
            run_as_of(data["inbox"][0]["now_runs"][0]).date().isoformat(),
            "2026-08-11",
        )
        attach_tickets_to_report(data, issues, kind="olap")
        item = data["inbox"][0]
        self.assertEqual(item["new_issue_count"], 1)
        self.assertEqual(item["queries"][0]["ticket_coverage"], "uncovered")
        self.assertTrue(item["queries"][0]["tickets"][0].get("post_close"))
        run = item["now_runs"][0]
        self.assertEqual(run["ticket_coverage"], "uncovered")
        self.assertIn("Query01", run["uncovered_queries"])
        self.assertEqual(data["summary"]["new_issues"], 1)

    def test_attach_passes_closed_at(self):
        issues = [
            {
                "number": 99,
                "title": "recent closed",
                "url": "https://example/99",
                "kind": "olap",
                "state": "closed",
                "closed_at": "2026-08-01T12:00:00Z",
                "fingerprint": "x",
                "keys": ["x"],
                "affected": [
                    {"suite": "UploadTpch100", "db": "sas_small_column", "queries": ["Query06"]}
                ],
            }
        ]
        data = {"inbox": [], "ok": []}
        attach_tickets_to_report(data, issues, kind="olap")
        self.assertEqual(data["known_issues"][0]["closed_at"], "2026-08-01T12:00:00Z")

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


class CoverageTests(unittest.TestCase):
    def _issue(self, **kw):
        base = {
            "number": 1,
            "title": "T",
            "url": "https://example/1",
            "kind": "olap",
            "fingerprint": "x",
            "keys": ["x"],
            "labels": ["main", "performance"],
            "affected": [
                {
                    "suite": "Tpcds1",
                    "db": "sas_big_column",
                    "queries": ["Query62"],
                }
            ],
        }
        base.update(kw)
        return base

    def test_norm_query_and_branch(self):
        self.assertEqual(norm_query_name("Tpcds1.Query62"), "Query62")
        self.assertEqual(norm_branch_label("trunk"), "main")
        self.assertEqual(norm_branch_label("origin/stable-26-3-1"), "stable-26-3-1")
        self.assertTrue(branch_label_match(["main"], "trunk"))
        self.assertFalse(branch_label_match(["stable-26-3-1"], "main"))

    def test_covered_on_branch(self):
        cov = classify_fail_coverage(
            [self._issue()],
            suite="Tpcds1",
            db="sas_big_column",
            branch="main",
            query="Query62",
            kind="olap",
        )
        self.assertEqual(cov["status"], "covered")
        self.assertEqual(cov["tickets"][0]["number"], 1)

    def test_trunk_alias_main_label(self):
        cov = classify_fail_coverage(
            [self._issue()],
            suite="Tpcds1",
            db="sas_big_column",
            branch="trunk",
            query="Query62",
        )
        self.assertEqual(cov["status"], "covered")

    def test_wrong_branch(self):
        cov = classify_fail_coverage(
            [self._issue(labels=["stable-26-3-1", "performance"])],
            suite="Tpcds1",
            db="sas_big_column",
            branch="main",
            query="Query62",
        )
        self.assertEqual(cov["status"], "wrong_branch")
        self.assertEqual(cov["missing_branch"], "main")
        self.assertEqual(cov["tickets"][0]["needs_branch"], "main")

    def test_wrong_branch_counts_as_new_issue(self):
        """needs <branch> label = still open for this branch → new_issue_count."""
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch100",
                    "db": "sas_small_column",
                    "branch": "stable-26-3",
                    "issue": "failing",
                    "bad_queries": [{"test": "Query03", "kind": "fail"}],
                    "now_runs": [],
                }
            ],
            "ok": [],
            "summary": {},
        }
        issues = [
            self._issue(
                number=47870,
                labels=["main", "performance"],
                affected=[
                    {
                        "suite": "UploadTpch100",
                        "db": "sas_small_column",
                        "queries": ["Query03"],
                    }
                ],
            )
        ]
        attach_tickets_to_report(data, issues, kind="olap")
        item = data["inbox"][0]
        self.assertEqual(item["wrong_branch_count"], 1)
        self.assertEqual(item["new_issue_count"], 1)
        self.assertEqual(data["summary"]["new_issues"], 1)
        self.assertEqual(item["bad_queries"][0]["ticket_coverage"], "wrong_branch")

    def test_uncovered_other_query(self):
        cov = classify_fail_coverage(
            [self._issue()],
            suite="Tpcds1",
            db="sas_big_column",
            branch="main",
            query="Query01",
        )
        self.assertEqual(cov["status"], "uncovered")

    def test_suite_wide_queries_empty(self):
        iss = self._issue(
            affected=[{"suite": "Tpcds1", "db": "sas_big_column", "queries": []}]
        )
        cov = classify_fail_coverage(
            [iss],
            suite="Tpcds1",
            db="sas_big_column",
            branch="main",
            query="Query99",
        )
        self.assertEqual(cov["status"], "covered")

    def test_aggregate_run_worst_uncovered(self):
        agg = aggregate_run_coverage(
            [
                {"status": "covered", "tickets": [{"number": 1, "branch_match": True}], "query": "Query01"},
                {"status": "uncovered", "tickets": [], "query": "Query02"},
            ],
            fail_count=2,
        )
        self.assertEqual(agg["ticket_coverage"], "uncovered")
        self.assertEqual(agg["uncovered_queries"], ["Query02"])
        self.assertEqual(agg["tickets"][0]["number"], 1)

    def test_suite_tickets_scoped_to_gap_queries(self):
        """Inbox suite pills = tickets for current fail/nodata only, not all suite history."""
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch1000",
                    "db": "sas_small_column",
                    "branch": "stable-26-3-1",
                    "issue": "failing",
                    "bad_queries": [{"test": "Query01", "kind": "fail"}],
                    "queries": [
                        {"test": "Query01", "kind": "fail"},
                        {"test": "Query05", "kind": "ok"},
                    ],
                }
            ],
            "ok": [],
            "summary": {},
        }
        issues = [
            self._issue(
                number=47284,
                labels=["main", "stable-26-3-1"],
                affected=[
                    {
                        "suite": "UploadTpch1000",
                        "db": "sas_small_column",
                        "queries": ["Query01"],
                    }
                ],
            ),
            self._issue(
                number=48261,
                labels=["main"],
                fingerprint="AppendSlice",
                keys=["AppendSlice"],
                affected=[
                    {
                        "suite": "UploadTpch1000",
                        "db": "sas_small_column",
                        "queries": ["Query05", "Query03"],
                    }
                ],
            ),
        ]
        attach_tickets_to_report(data, issues, kind="olap")
        item = data["inbox"][0]
        self.assertEqual([t["number"] for t in item["tickets"]], [47284])
        self.assertEqual(
            sorted(t["number"] for t in item["suite_tickets"]),
            [47284, 48261],
        )
        qmap = {q["test"]: q for q in item["queries"]}
        self.assertEqual(qmap["Query01"]["tickets"][0]["number"], 47284)
        self.assertEqual(qmap["Query05"]["tickets"][0]["number"], 48261)
        self.assertEqual(qmap["Query05"]["tickets"][0]["needs_branch"], "stable-26-3-1")

    def test_attach_now_runs_and_new_fail(self):
        data = {
            "inbox": [
                {
                    "suite": "Tpcds1",
                    "db": "sas_big_column",
                    "branch": "main",
                    "issue": "failing",
                    "bad_queries": [
                        {"test": "Query62", "kind": "fail"},
                        {"test": "Query01", "kind": "fail"},
                    ],
                    "now_runs": [
                        {
                            "fail": 2,
                            "fail_tests": "Query62,Query01",
                            "fail_rate": 0.02,
                        }
                    ],
                }
            ],
            "ok": [],
            "summary": {},
        }
        issues = [self._issue(number=48234, labels=["main"])]
        attach_tickets_to_report(data, issues, kind="olap")
        item = data["inbox"][0]
        self.assertEqual(item["new_issue_count"], 1)  # Query01 uncovered
        self.assertEqual(item["wrong_branch_count"], 0)
        bq = {q["test"]: q for q in item["bad_queries"]}
        self.assertEqual(bq["Query62"]["ticket_coverage"], "covered")
        self.assertEqual(bq["Query01"]["ticket_coverage"], "uncovered")
        run = item["now_runs"][0]
        self.assertEqual(run["ticket_coverage"], "uncovered")
        self.assertIn(48234, [t["number"] for t in run["tickets"]])
        self.assertEqual(data["summary"]["new_issues"], 1)

    def test_attach_finished_twin_now_runs(self):
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch1000",
                    "db": "sas_small_column",
                    "branch": "main",
                    "issue": "in_progress",
                    "now_runs": [],
                    "finished": {
                        "issue": "failing",
                        "status": "failing",
                        "now_runs": [
                            {"fail": 1, "fail_tests": "01", "fail_rate": 0.04},
                            {"fail": 1, "fail_tests": "05", "fail_rate": 0.04},
                        ],
                        "bad_queries": [{"test": "Query05", "kind": "fail"}],
                    },
                }
            ],
            "ok": [],
            "summary": {},
        }
        issues = [
            self._issue(
                number=47284,
                labels=["main"],
                affected=[
                    {
                        "suite": "UploadTpch1000",
                        "db": "sas_small_column",
                        "queries": ["Query01"],
                    }
                ],
            )
        ]
        attach_tickets_to_report(data, issues, kind="olap")
        fin = data["inbox"][0]["finished"]
        r0, r1 = fin["now_runs"]
        self.assertEqual(r0["ticket_coverage"], "covered")
        self.assertEqual(r0["tickets"][0]["number"], 47284)
        self.assertEqual(r1["ticket_coverage"], "uncovered")
        self.assertEqual(r1["uncovered_queries"], ["Query05"])
        self.assertGreaterEqual(fin["new_issue_count"], 1)

    def test_nodata_counts_as_new_issue(self):
        data = {
            "inbox": [
                {
                    "suite": "Tpch1000",
                    "db": "sas_big_column",
                    "branch": "main",
                    "issue": "ok",
                    "bad_queries": [
                        {"test": "Query21", "kind": "nodata"},
                        {"test": "Query22", "kind": "nodata"},
                    ],
                    "now_runs": [],
                }
            ],
            "ok": [],
            "summary": {},
        }
        attach_tickets_to_report(data, [], kind="olap")
        self.assertEqual(data["inbox"][0]["new_issue_count"], 2)
        self.assertEqual(data["summary"]["new_issues"], 1)

    def test_nodata_on_now_run_badge(self):
        """Last-runs card gets no-ticket when catalog/history has nodata (fail=0)."""
        data = {
            "inbox": [
                {
                    "suite": "Tpch1000",
                    "db": "sas_big_column",
                    "branch": "main",
                    "issue": "ok",
                    "bad_queries": [
                        {"test": "Query21", "kind": "nodata"},
                        {"test": "Query22", "kind": "nodata"},
                    ],
                    "queries": [
                        {
                            "test": "Query21",
                            "kind": "nodata",
                            "history": {
                                "labels": ["2026-07-28T10:00:00", "2026-07-29T10:00:00"],
                                "versions": ["aaaaaaaa", "18a32d4"],
                                "ci_versions": ["trunk.r1", "trunk.r2"],
                                "nodata": [False, True],
                                "fail_rate": [0.0, None],
                                "ydb": [100.0, None],
                            },
                        },
                        {
                            "test": "Query22",
                            "kind": "nodata",
                            "history": {
                                "labels": ["2026-07-28T10:00:00", "2026-07-29T10:00:00"],
                                "versions": ["aaaaaaaa", "18a32d4"],
                                "ci_versions": ["trunk.r1", "trunk.r2"],
                                "nodata": [False, True],
                                "fail_rate": [0.0, None],
                                "ydb": [100.0, None],
                            },
                        },
                    ],
                    "now_runs": [
                        {
                            "ts": "2026-07-28T10:00:00",
                            "version": "aaaaaaaa",
                            "ci_version": "trunk.r1",
                            "fail": 0,
                            "fail_tests": "",
                        },
                        {
                            "ts": "2026-07-29T10:00:00",
                            "version": "18a32d4",
                            "ci_version": "trunk.r2",
                            "fail": 0,
                            "fail_tests": "",
                        },
                    ],
                }
            ],
            "ok": [],
            "summary": {},
        }
        attach_tickets_to_report(data, [], kind="olap")
        r0, r1 = data["inbox"][0]["now_runs"]
        self.assertEqual(r0["ticket_coverage"], "ok")
        self.assertEqual(r1["ticket_coverage"], "uncovered")
        self.assertIn("Query21", r1["uncovered_queries"])
        self.assertIn("Query22", r1["uncovered_queries"])


class RecentlyClosedFilterTests(unittest.TestCase):
    def test_closed_issues_since_date(self):
        now = datetime(2026, 8, 6, 15, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(closed_issues_since_date(now=now, max_age_days=10), "2026-07-27")
        self.assertEqual(CLOSED_ISSUES_MAX_AGE_DAYS, 10)

    def test_parse_github_ts(self):
        dt = parse_github_ts("2026-08-01T12:00:00Z")
        self.assertIsNotNone(dt)
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt.day, 1)
        self.assertIsNone(parse_github_ts(None))
        self.assertIsNone(parse_github_ts("not-a-date"))

    def test_is_recently_closed_keeps_fresh(self):
        now = datetime(2026, 8, 6, 12, 0, 0, tzinfo=timezone.utc)
        iss = {
            "state": "closed",
            "closed_at": (now - timedelta(days=3)).isoformat().replace("+00:00", "Z"),
        }
        self.assertTrue(is_recently_closed(iss, now=now))

    def test_is_recently_closed_drops_old(self):
        now = datetime(2026, 8, 6, 12, 0, 0, tzinfo=timezone.utc)
        iss = {
            "state": "closed",
            "closed_at": (now - timedelta(days=20)).isoformat().replace("+00:00", "Z"),
        }
        self.assertFalse(is_recently_closed(iss, now=now))

    def test_is_recently_closed_drops_missing_closed_at(self):
        self.assertFalse(is_recently_closed({"state": "closed"}, now=datetime.now(timezone.utc)))

    def test_open_never_recently_closed(self):
        now = datetime(2026, 8, 6, 12, 0, 0, tzinfo=timezone.utc)
        self.assertFalse(
            is_recently_closed(
                {"state": "open", "closed_at": now.isoformat()},
                now=now,
            )
        )


class DutyDecisionsAttachTests(unittest.TestCase):
    def test_attach_wait_next_wave_on_matching_label(self):
        label = "2026-08-05_c460199"
        fk = focus_key(
            kind="olap",
            branch="stable-26-3-1",
            db="sas_small_column",
            suite="UploadTpch1000",
            label=label,
        )
        decision = {
            "focus_key": fk,
            "resolution": "wait_next_wave",
            "kind": "olap",
            "branch": "stable-26-3-1",
            "db": "sas_small_column",
            "suite": "UploadTpch1000",
            "label": label,
            "analysis_url": "https://example/analysis.md",
            "summary": "IC cascade",
            "queries": ["Query18"],
        }
        index = merge_decision_into_index(empty_index(), decision)
        data = {
            "inbox": [
                {
                    "suite": "UploadTpch1000",
                    "db": "sas_small_column",
                    "branch": "stable-26-3-1",
                    "now_runs": [
                        {"label": "2026-08-04_old", "fail": 1},
                        {"label": label, "fail": 1},
                    ],
                },
                {
                    "suite": "UploadTpch1000",
                    "db": "vla_small_column",
                    "branch": "main",
                    "now_runs": [{"label": label, "fail": 1}],
                },
            ]
        }
        n = attach_duty_decisions_to_report(data, index, kind="olap")
        self.assertEqual(n, 1)
        run = data["inbox"][0]["now_runs"][1]
        self.assertEqual(run["duty_decision"]["analysis_url"], "https://example/analysis.md")
        self.assertEqual(run["duty_decision"]["queries"], ["Query18"])
        self.assertEqual(
            data["inbox"][0]["duty_decision"]["resolution"], "wait_next_wave"
        )
        self.assertNotIn("duty_decision", data["inbox"][1]["now_runs"][0])
        self.assertEqual(len(data.get("duty_decisions") or []), 1)


if __name__ == "__main__":
    unittest.main()
