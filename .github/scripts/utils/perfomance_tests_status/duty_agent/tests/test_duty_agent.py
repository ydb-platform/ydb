#!/usr/bin/env python3
"""Unit tests for dutyctl toolbox (facts + validate; no autopilot RCA)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

TESTS = Path(__file__).resolve().parent
AGENT = TESTS.parent
FIXTURES = TESTS / "fixtures"
sys.path.insert(0, str(AGENT))

from tools.attachments import (  # noqa: E402
    extract_host_dig_hints,
    pick_priority_attachments,
    scan_log_text,
    summarize_plan_text,
)
from tools.baseline import (  # noqa: E402
    compare_plan_digs,
    select_baseline_from_pack_history,
    select_baseline_from_slice_runs,
)
from tools.sandbox import _name_matches  # noqa: E402
from tools.trace import (  # noqa: E402
    ensure_trace_in_analysis,
    inject_into_analysis,
    record as trace_record,
    rebuild_from_artifacts,
    render_ascii_tree,
    _dedupe_root_stages,
)
from tools.context import (  # noqa: E402
    ContextError,
    focus_report_local,
    load_context,
    load_context_pack,
    validate_context,
)
from tools.detect_type import detect_type  # noqa: E402
from tools.dig_runs import (  # noqa: E402
    build_dig_sql,
    rows_from_mcp_json,
    summarize_olap_rows,
    summarize_tpcc_rows,
)
from tools.metrics_delta import metrics_delta  # noqa: E402
from tools.result_json import merge_result  # noqa: E402
from tools.run_dir import ensure_run_dir, write_json  # noqa: E402
from tools.sandbox import extract_fingerprints, inspect_sandbox  # noqa: E402
from tools.validate_report import validate_analysis_md  # noqa: E402
from tools.yav import read_token_config, token_specs_from_config  # noqa: E402


class ContextTests(unittest.TestCase):
    def test_load_olap(self):
        ctx = load_context(FIXTURES / "sample_olap.json")
        self.assertEqual(ctx["report"]["kind"], "olap")

    def test_load_tpcc(self):
        ctx = load_context(FIXTURES / "sample_tpcc.json")
        self.assertEqual(ctx["report"]["kind"], "tpcc")

    def test_reject_bad_schema(self):
        with self.assertRaises(ContextError):
            validate_context({"schema": "nope", "report": {"kind": "olap"}, "selection": {}})


class DetectTypeTests(unittest.TestCase):
    def test_olap_fail_seed(self):
        ctx = load_context(FIXTURES / "sample_olap.json")
        det = detect_type(ctx)
        self.assertEqual(det["rollup"], "olap_fail")
        self.assertIn("olap_fail", det["analysis_types"])
        self.assertTrue(any(p.get("test") == "Query05" for p in det["problems_seed"]))

    def test_tpcc_mixed_lat_tpmc(self):
        ctx = load_context(FIXTURES / "sample_tpcc.json")
        det = detect_type(ctx)
        self.assertEqual(det["rollup"], "mixed")
        self.assertIn("tpcc_tpmc", det["analysis_types"])
        self.assertIn("tpcc_lat", det["analysis_types"])

    def test_olap_nodata_from_query_counts(self):
        ctx = load_context(FIXTURES / "sample_olap.json")
        ctx = json.loads(json.dumps(ctx))
        ctx["suite_now"] = {
            **ctx["suite_now"],
            "issue": "ok",
            "status": "ok",
            "fail_rate_now": 0,
            "ydb_pct": -71.0,
            "n_nodata": 73,
            "n_ok": 26,
            "n_queries": 99,
            "query_counts": {
                "fail": 0,
                "slow": 0,
                "soft": 0,
                "nodata": 73,
                "ok": 26,
                "total": 99,
            },
        }
        ctx["queries"] = [
            {"test": "Query27", "kind": "nodata"},
            {"test": "Query28", "kind": "nodata"},
        ]
        ctx["selection"]["focus_run"] = {
            **(ctx["selection"].get("focus_run") or {}),
            "success": 26,
            "fail": 0,
            "ydb": 51518.0,
        }
        det = detect_type(ctx)
        self.assertIn("olap_nodata", det["analysis_types"])
        self.assertEqual(det["query_counts"]["nodata"], 73)
        self.assertTrue(
            any(p.get("analysis_type") == "olap_nodata" for p in det["problems_seed"])
        )

    def test_olap_nodata_legacy_pack_empty_queries(self):
        """Old Save omitted nodata queries — still seed from SuccessCount / ydb collapse."""
        ctx = load_context(FIXTURES / "sample_olap.json")
        ctx = json.loads(json.dumps(ctx))
        ctx["suite_now"] = {
            "issue": "ok",
            "status": "ok",
            "fail_rate_now": 0,
            "ydb_pct": -71.0,
            "ydb_now": 51518.0,
            "ydb_base": 178000.0,
            "reasons": [],
        }
        ctx["queries"] = []
        ctx["sticky_query"] = None
        ctx["selection"]["focus_run"] = {
            "label": "2026-07-26_4ec357a",
            "sha": "4ec357a",
            "success": 26,
            "fail": 0,
            "ydb": 51518.0,
            "report": "https://proxy.sandbox.yandex-team.ru/12927819679/index.html",
        }
        det = detect_type(ctx)
        self.assertIn("olap_nodata", det["analysis_types"])
        self.assertNotIn("olap_fail", det["analysis_types"])

    def test_uncovered_and_compare_priority_notes(self):
        ctx = load_context(FIXTURES / "sample_olap.json")
        ctx = json.loads(json.dumps(ctx))
        ctx["ticket_coverage"] = {
            "status": "uncovered",
            "uncovered_queries": ["Query05"],
            "wrong_branch_queries": [],
            "investigate_uncovered_first": True,
        }
        ctx["queries"][0]["ticket_coverage"] = "uncovered"
        ctx["compare"] = {
            "wave_id": "trunk.r1",
            "active": True,
            "label": "2026-07-26_abc1234",
            "run": {
                "sha": "abc1234",
                "label": "2026-07-26_abc1234",
                "report": "https://example.test/cmp",
            },
            "queries": [
                {
                    "test": "Query08",
                    "kind": "fail",
                    "ticket_coverage": "uncovered",
                }
            ],
        }
        ctx["hints"] = {
            "react": ["fail", "new"],
            "investigate_uncovered_first": True,
            "compare_active": True,
        }
        det = detect_type(ctx)
        self.assertTrue(det["ticket_coverage"]["investigate_uncovered_first"])
        self.assertIn("Query05", det["ticket_coverage"]["uncovered_queries"])
        self.assertTrue(det["compare_active"])
        self.assertIn("PRIORITY", det["note"])
        self.assertIn("MANDATORY", det["note"])
        self.assertIn("compare.run", det["note"])
        self.assertGreaterEqual(det.get("compare_fail_seeded") or 0, 1)
        self.assertIn("olap_fail", det["analysis_types"])
        cmp_seeds = [
            s
            for s in det["problems_seed"]
            if isinstance(s, dict) and s.get("source") == "compare.run"
        ]
        self.assertTrue(cmp_seeds)
        self.assertEqual(cmp_seeds[0].get("test"), "Query08")
        self.assertIn("Query08", det["ticket_coverage"]["uncovered_queries"])


class MetricsDeltaTests(unittest.TestCase):
    def test_tpcc_flags(self):
        ctx = load_context(FIXTURES / "sample_tpcc.json")
        m = metrics_delta(ctx)
        self.assertIn("lat_regression", m["flags"])
        self.assertIn("tpmc_regression", m["flags"])


class DigRunsTests(unittest.TestCase):
    def test_build_tpcc_sql_and_summarize(self):
        ctx = load_context(FIXTURES / "sample_tpcc.json")
        plan = build_dig_sql(ctx, neighbors=True, days_before=35)
        self.assertEqual(plan["kind"], "tpcc")
        self.assertEqual(plan["days_before"], 35)
        self.assertIn("perfomance/tpcc", plan["sql"])
        self.assertIn("ydb_cli_", plan["sql"])
        self.assertIn("git_branch", plan["sql"])
        # Pack run_type "default" → mart ydb_cli_serializable_default
        self.assertEqual(plan["selection"]["run_type"], "ydb_cli_serializable_default")
        raw = {
            "result_sets": [
                {
                    "columns": [
                        {"name": "cluster"},
                        {"name": "run_type"},
                        {"name": "warehouses"},
                        {"name": "git_branch"},
                        {"name": "timestamp"},
                        {"name": "tpmC"},
                        {"name": "lat90"},
                        {"name": "efficiency"},
                        {"name": "version"},
                    ],
                    "rows": [
                        [
                            "perf9",
                            "ydb_cli_serializable_default",
                            16000,
                            "origin/main",
                            "2026-07-15T10:00:00Z",
                            200000,
                            4000,
                            0.9,
                            "aaa1111",
                        ],
                        [
                            "perf9",
                            "ydb_cli_snapshot_latency",
                            12000,
                            "origin/main",
                            "2026-07-15T12:00:00Z",
                            50000,
                            40,
                            0.9,
                            "aaa1111",
                        ],
                        [
                            "perf9",
                            "ydb_cli_serializable_default",
                            16000,
                            "origin/main",
                            "2026-07-16T10:00:00Z",
                            179657,
                            5174,
                            0.85,
                            "e5c6883d5449",
                        ],
                        [
                            "perf3",
                            "ydb_cli_serializable_default",
                            16000,
                            "origin/main",
                            "2026-07-16T11:00:00Z",
                            210000,
                            4100,
                            0.9,
                            "e5c6883d5449",
                        ],
                    ],
                }
            ]
        }
        rows = rows_from_mcp_json(raw)
        self.assertEqual(len(rows), 4)
        summary = summarize_tpcc_rows(rows, selection=plan["selection"])
        # Focus suite only — latency row must not inflate slice / jump
        self.assertEqual(summary["slice_count"], 2)
        self.assertIsNotNone(summary["largest_lat_step"])
        self.assertGreater(summary["largest_lat_step"]["lat_delta"], 0)
        self.assertTrue(any(p["cluster"] == "perf3" for p in summary["peer_clusters_latest"]))
        self.assertTrue(any(c["suite"].startswith("ydb_cli_snapshot_latency") for c in summary["cross_run_type"]))

    def test_build_olap_sql_neighbors(self):
        ctx = load_context(FIXTURES / "sample_olap.json")
        plan = build_dig_sql(ctx, neighbors=True, days_before=40)
        self.assertEqual(plan["kind"], "olap")
        self.assertIn("fast_results_siutes", plan["sql"])
        self.assertIn("StartsWith(Suite", plan["sql"])
        self.assertIn("Branch", plan["sql"])
        # Peer DBs: no DbAlias filter in neighbors mode
        self.assertNotIn("DbAlias =", plan["sql"])
        raw = {
            "result_sets": [
                {
                    "columns": [
                        {"name": "Branch"},
                        {"name": "Version"},
                        {"name": "DbAlias"},
                        {"name": "Suite"},
                        {"name": "RunTs"},
                        {"name": "YdbSumMeans"},
                        {"name": "FailCount"},
                        {"name": "Report"},
                    ],
                    "rows": [
                        [
                            "origin/main",
                            "aaaaaaa",
                            "sas_small_column",
                            "TpchParallelS100T10",
                            "2026-07-01T05:00:00Z",
                            100.0,
                            0,
                            None,
                        ],
                        [
                            "origin/main",
                            "bbbbbbb",
                            "sas_small_column",
                            "TpchParallelS100T10",
                            "2026-07-08T05:00:00Z",
                            120.0,
                            5,
                            None,
                        ],
                        [
                            "origin/main",
                            "bbbbbbb",
                            "sas_big_column",
                            "TpchParallelS100T10",
                            "2026-07-08T06:00:00Z",
                            110.0,
                            0,
                            None,
                        ],
                        [
                            "origin/main",
                            "bbbbbbb",
                            "sas_small_column",
                            "UploadTpch100",
                            "2026-07-08T07:00:00Z",
                            50.0,
                            1,
                            None,
                        ],
                    ],
                }
            ]
        }
        rows = rows_from_mcp_json(raw)
        summary = summarize_olap_rows(rows, selection=plan["selection"])
        self.assertEqual(summary["slice_count"], 2)
        self.assertIsNotNone(summary["largest_fail_step"])
        self.assertGreater(summary["largest_fail_step"]["delta"], 0)
        self.assertTrue(any(p["DbAlias"] == "sas_big_column" for p in summary["peer_dbs"]))
        self.assertTrue(any(c["suite"] == "UploadTpch100" for c in summary["cross_suite"]))
        # suite-stable plateau end → pr_window for dig-prs (not pack prev-green)
        self.assertIsNotNone(summary.get("pr_window"))
        self.assertIn(summary["pr_window"]["source"], (
            "stable_streak_end",
            "stable_streak_end_weak",
        ))
        self.assertTrue(str(summary["pr_window"]["head"]).endswith("bbbbbbb") or "bbbbbbb" in str(summary["pr_window"]["head"]))

    def test_olap_pr_window_prefers_stable_streak_not_ancient_fail_step(self):
        from tools.dig_runs import build_olap_pr_window

        # Three calm greens, then red — plateau end = main.s3 (not ancient jump).
        runs = [
            {"RunTs": "t0", "Version": "main.oldfail1", "FailCount": 0, "YdbSumMeans": 100},
            {"RunTs": "t1", "Version": "main.oldfail2", "FailCount": 1, "YdbSumMeans": 90},
            {"RunTs": "t2", "Version": "main.s1", "FailCount": 0, "YdbSumMeans": 105},
            {"RunTs": "t3", "Version": "main.s2", "FailCount": 0, "YdbSumMeans": 110},
            {"RunTs": "t4", "Version": "main.s3", "FailCount": 0, "YdbSumMeans": 108},
            {"RunTs": "t5", "Version": "main.focus", "FailCount": 1, "YdbSumMeans": 80},
        ]
        fail_jump = {
            "from_version": "main.oldfail1",
            "to_version": "main.oldfail2",
            "delta": 1,
        }
        pw = build_olap_pr_window(runs, fail_jump=fail_jump, ydb_jump=None)
        self.assertEqual(pw["source"], "stable_streak_end")
        self.assertEqual(pw["base"], "main.s3")
        self.assertEqual(pw["head"], "main.focus")
        self.assertEqual(pw["streak_len"], 3)

    def test_olap_pr_window_skips_fluke_green_in_red_streak(self):
        """Nearest FailCount=0 in a red streak is not a stable baseline."""
        from tools.dig_runs import build_olap_pr_window, find_olap_stable_plateau

        runs = [
            {"RunTs": "t0", "Version": "main.a", "FailCount": 0, "YdbSumMeans": 140},
            {"RunTs": "t1", "Version": "main.b", "FailCount": 0, "YdbSumMeans": 135},
            {"RunTs": "t2", "Version": "main.c", "FailCount": 0, "YdbSumMeans": 142},
            {"RunTs": "t3", "Version": "main.d", "FailCount": 1, "YdbSumMeans": 120},
            {"RunTs": "t4", "Version": "main.e", "FailCount": 1, "YdbSumMeans": 118},
            {"RunTs": "t5", "Version": "main.f", "FailCount": 1, "YdbSumMeans": 112},
            {"RunTs": "t6", "Version": "main.fluke", "FailCount": 0, "YdbSumMeans": 141},
            {"RunTs": "t7", "Version": "main.focus", "FailCount": 1, "YdbSumMeans": 113},
        ]
        plate = find_olap_stable_plateau(runs)
        self.assertEqual(plate["Version"], "main.c")
        self.assertEqual(plate["streak_len"], 3)
        pw = build_olap_pr_window(runs, fail_jump=None, ydb_jump=None)
        self.assertEqual(pw["base"], "main.c")
        self.assertNotEqual(pw["base"], "main.fluke")
        self.assertEqual(pw["source"], "stable_streak_end")


class ValidateTests(unittest.TestCase):
    def test_olap_slow_requires_plan_and_baseline(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_slow"]})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {}})
            write_json(d / "focus.json", {"fetched": False, "slow_query_names": ["Query03"]})
            md = """# Perf duty — x

## Заключение
- **Итог:** suite slower by ydb_pct
- **Решение:** wait_next_wave
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** прогон 2026-07-26_x
- **Механика:** ydb wall time up

## Проблемы
### P1 — slow
- Тип: olap_slow
- Логи: kikimr__stderr empty; kikimr__logs empty
- Код ([`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)): n/a
- Гипотеза проверена: no

## Что дальше
1. ждать

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/12927692288/index.html
[`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            blob = " ".join(r["errors"]).lower()
            self.assertIn("plan", blob)
            self.assertTrue("iteration" in blob or "итерац" in blob or "baseline" in blob)
            self.assertIn("dig_prs", blob)
            self.assertIn("bisect", blob)

    def test_olap_fail_segfault_requires_coredump_dig(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(
                d / "focus.json",
                {
                    "fetched": True,
                    "fatal": {"signals": ["segfault", "unavailable"], "coredump_urls": [
                        "https://coredumps.yandex-team.ru/v3/cores/86b7f24834f1489abf7a67d56057c595"
                    ]},
                    "allure": {
                        "cases": [
                            {
                                "name": "UploadTpch100.Query03",
                                "attach_analysis": {
                                    "signals": ["segfault"],
                                    "host_dig": {
                                        "coredump_urls": [
                                            "https://coredumps.yandex-team.ru/v3/cores/"
                                            "86b7f24834f1489abf7a67d56057c595"
                                        ]
                                    },
                                },
                            }
                        ]
                    },
                },
            )
            write_json(d / "code_bisect.json", {"introduced_in_window": None})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {}})
            md = """# Perf duty — x

## Заключение
- **Итог:** code 2005 without stack dig
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** прогон 2026-07-26_631ab94
- **Механика:** node lost

## Проблемы
### P1 — q
- Тип: olap_fail
- Логи: kikimr__stderr signal mentioned vaguely; kikimr__logs connection lost
- Код ([`631ab94`](https://github.com/ydb-platform/ydb/commit/631ab94)): `ydb/core/`
- Гипотеза проверена: no

## Что дальше
1. ещё логи

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/12927692288/index.html
[`631ab94`](https://github.com/ydb-platform/ydb/commit/631ab94)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(any("coredump" in e.lower() for e in r["errors"]), r["errors"])

    def test_olap_nodata_must_be_discussed(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "detect_type.json",
                {
                    "analysis_types": ["olap_nodata"],
                    "query_counts": {"nodata": 73, "ok": 26, "total": 99},
                    "problems_seed": [
                        {"id": "seed_suite_nodata", "analysis_type": "olap_nodata", "title": "no data ×73"}
                    ],
                },
            )
            write_json(
                d / "problems.json",
                {"items": [{"id": "p_fail_only", "analysis_type": "olap_fail", "title": "other suite fail"}]},
            )
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 1}})
            md = """# Perf duty — x

## Заключение
- **Итог:** только fail соседнего suite
- **Решение:** wait_next_wave
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** на прогоне 2026-07-26_4ec357a
- **Механика:** code 2005

## Проблемы
### P1 — other
- Тип: olap_fail
- Что сломалось: fail
- Почему / механика: x
- Логи: kikimr__stderr empty; kikimr__logs connection lost
- Код ([`4ec357a`](https://github.com/ydb-platform/ydb/commit/4ec357a)): n/a
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: partial
- Связанный issue: нет
- Тикет: нет

## Что дальше
1. ждать

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/12927819679/index.html
[`4ec357a`](https://github.com/ydb-platform/ydb/commit/4ec357a)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            blob = " ".join(r["errors"])
            self.assertIn("olap_nodata", blob)

    def test_olap_nodata_ok_with_report_lag_branch(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "detect_type.json",
                {"analysis_types": ["olap_nodata"], "query_counts": {"nodata": 73, "ok": 26}},
            )
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "id": "p1",
                            "analysis_type": "olap_nodata",
                            "title": "Tpcds1 no data ×73",
                        }
                    ]
                },
            )
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 1}})
            write_json(
                d / "s3_report.json",
                {
                    "run_id": "x",
                    "stamp": "20260101T000000Z",
                    "analysis_url": "https://storage.yandexcloud.net/workload-log/x/analysis.md",
                },
            )
            md = """# Perf duty — x

## Заключение
- **Итог:** Now nodata 73; в Allure эти query ok — в базу ещё не доехали
- **Решение:** wait_next_wave
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** прогон 2026-07-26_4ec357a
- **Механика:** лаг выгрузки daily/mart после успешного отчёта

## Проблемы
### P1 — nodata lag
- Тип: olap_nodata
- Что сломалось: в Now нет Query27+
- Почему / механика: в отчёте ok → данные ещё не доехали в базу
- Логи: не копали (ветка lag)
- Код ([`4ec357a`](https://github.com/ydb-platform/ydb/commit/4ec357a)): n/a
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: yes
- Связанный issue: нет
- Тикет: нет

## Что дальше
1. refresh mart

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/12927819679/index.html
[`4ec357a`](https://github.com/ydb-platform/ydb/commit/4ec357a)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertTrue(r["ok"], r["errors"])

    def test_nodata_tail_must_be_in_match_affected(self):
        """Abort cut-off nodata must land in same issue affected (Materials match)."""
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "detect_type.json",
                {
                    "analysis_types": ["olap_fail", "olap_nodata"],
                    "query_counts": {"fail": 1, "nodata": 2, "ok": 0},
                    "problems_seed": [
                        {
                            "id": "p_fail",
                            "analysis_type": "olap_fail",
                            "title": "Query03 SIGSEGV",
                            "test": "Query03",
                        },
                        {
                            "id": "p_nd",
                            "analysis_type": "olap_nodata",
                            "title": "no data ×2",
                        },
                    ],
                },
            )
            write_json(
                d / "context.json",
                {
                    "queries": [
                        {"test": "Query03", "kind": "fail"},
                        {"test": "Query17", "kind": "nodata"},
                        {"test": "Query18", "kind": "nodata"},
                    ]
                },
            )
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "id": "p1",
                            "analysis_type": "olap_fail",
                            "title": "Query03 crash",
                        },
                        {
                            "id": "p2",
                            "analysis_type": "olap_nodata",
                            "title": "Query17+ nodata следствие abort",
                            "queries": ["Query17", "Query18"],
                        },
                    ]
                },
            )
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 2}})
            base = """# Perf duty — x

## Заключение
- **Проблема:** Query03 VERIFY abort; хвост Query17/18 nodata
- **Из‑за чего:** crash compaction AppendSlice; nodata — следствие cut-off
- **Чинить:** уже [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- **Решение:** update_known
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне

## Проблемы
### P1 — crash
- Тип: olap_fail
- Что сломалось: VERIFY Query03
- Почему / механика: abort
- Логи: kikimr__stderr VERIFY; kikimr__logs disconnect
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): AppendSlice
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: yes
- Связанный issue: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- Тикет: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
### P2 — nodata хвост
- Тип: olap_nodata
- Что сломалось: Query17 Query18 nodata
- Почему / механика: в отчёте тоже нет → следствие abort P1
- Логи: suite cut-off
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): n/a
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: yes
- Связанный issue: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- Тикет: [#48261](https://github.com/ydb-platform/ydb/issues/48261)

## Гипотезы происхождения
- **H1** (подтверждена): crash в compaction AppendSlice / PoolBuffer — [`merged_column.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/engines/changes/compaction/plain/merged_column.cpp#L8)
- Issues (поиск): fingerprint AppendSlice — учёл [#48261](https://github.com/ydb-platform/ydb/issues/48261); иных совпадений нет.

## Причины
- Повтор [#48261](https://github.com/ydb-platform/ydb/issues/48261); давний дефект проявился на Query03, хвост suite — следствие abort.

## Как починить
1. Как в [#48261](https://github.com/ydb-platform/ydb/issues/48261): чинить AppendSlice/PoolBuffer path.

## Что дальше
1. annotate-issue

## Материалы для issue
### Title
```
Comment: AppendSlice matches #48261
```
### Body
#### Фактура
| | |
|--|--|
| Suite / DB | `UploadTpch1000` / `sas_small_column` |
| Branch · Version | `main` · [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100) |
| Run | `2026-07-29_f88e100` · `2026-07-29T12:00:00` UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/12923171727/index.html |
| Failed | Query03
#### Что сломалось
VERIFY; nodata хвост — следствие.
#### К чему приводит
- Abort; cut-off suite.
#### Из‑за чего
crash compaction AppendSlice; nodata — следствие cut-off.
#### Чинить
уже [#48261](https://github.com/ydb-platform/ydb/issues/48261)
#### Детали ошибки
```
VERIFY AppendSlice
```
#### Код
| | |
|--|--|
| Место падения | AppendSlice |
| Связанный issue | [#48261](https://github.com/ydb-platform/ydb/issues/48261) |

<!-- perf-duty-match
kind: olap
fingerprint: AppendSlice
keys:
  - AppendSlice
affected:
  - suite: UploadTpch1000
    db: sas_small_column
    queries: [Query03]
-->
"""
            r = validate_analysis_md(base, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(
                any("nodata after abort" in e for e in r["errors"]),
                r["errors"],
            )
            fixed = base.replace(
                "queries: [Query03]",
                "queries: [Query03, Query17, Query18]",
            )
            r2 = validate_analysis_md(fixed, out_dir=d)
            self.assertTrue(r2["ok"], r2["errors"])

    def test_nodata_uncovered_queries_beyond_pack_sample(self):
        """ticket_coverage.uncovered_queries must be in affected even if not in queries[]."""
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "detect_type.json",
                {
                    "analysis_types": ["olap_fail", "olap_nodata"],
                    "query_counts": {"fail": 1, "nodata": 43, "ok": 0},
                    "problems_seed": [
                        {
                            "id": "p_fail",
                            "analysis_type": "olap_fail",
                            "title": "Query00 SIGSEGV",
                            "test": "Query00",
                        },
                        {
                            "id": "p_nd",
                            "analysis_type": "olap_nodata",
                            "title": "no data ×43",
                        },
                    ],
                },
            )
            write_json(
                d / "context.json",
                {
                    "suite_now": {"n_nodata": 43, "query_counts": {"nodata": 43}},
                    # Truncated sample (legacy Save context slice) — only Q00–Q01.
                    "queries": [
                        {"test": "Query00", "kind": "nodata"},
                        {"test": "Query01", "kind": "nodata"},
                    ],
                    "ticket_coverage": {
                        "status": "uncovered",
                        "uncovered_queries": [
                            "Query00",
                            "Query01",
                            "Query29",
                            "Infrastructure error",
                        ],
                    },
                    "selection": {
                        "focus_run": {
                            "uncovered_queries": [
                                "Query00",
                                "Query01",
                                "Query29",
                                "Infrastructure error",
                            ]
                        }
                    },
                },
            )
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "id": "p1",
                            "analysis_type": "olap_fail",
                            "title": "Query00 crash",
                        },
                        {
                            "id": "p2",
                            "analysis_type": "olap_nodata",
                            "title": "no data ×43 следствие abort",
                            "queries": ["Query00", "Query01"],
                        },
                    ]
                },
            )
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 2}})
            base = """# Perf duty — x

## Заключение
- **Проблема:** Query00 VERIFY abort; suite nodata×43
- **Из‑за чего:** crash compaction AppendSlice; nodata — следствие cut-off
- **Чинить:** уже [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- **Решение:** update_known
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне

## Проблемы
### P1 — crash
- Тип: olap_fail
- Что сломалось: VERIFY Query00
- Почему / механика: abort
- Логи: kikimr__stderr VERIFY; kikimr__logs disconnect
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): AppendSlice
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: yes
- Связанный issue: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- Тикет: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
### P2 — nodata хвост
- Тип: olap_nodata
- Что сломалось: nodata×43
- Почему / механика: в отчёте тоже нет → следствие abort P1
- Логи: suite cut-off
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): n/a
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: yes
- Связанный issue: [#48261](https://github.com/ydb-platform/ydb/issues/48261)
- Тикет: [#48261](https://github.com/ydb-platform/ydb/issues/48261)

## Гипотезы происхождения
- **H1** (подтверждена): crash в compaction AppendSlice — [`merged_column.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/engines/changes/compaction/plain/merged_column.cpp#L8)
- Issues (поиск): fingerprint AppendSlice — учёл [#48261](https://github.com/ydb-platform/ydb/issues/48261); иных совпадений нет.

## Причины
- Повтор [#48261](https://github.com/ydb-platform/ydb/issues/48261); хвост suite — следствие abort.

## Как починить
1. Как в [#48261](https://github.com/ydb-platform/ydb/issues/48261): чинить AppendSlice path.

## Что дальше
1. annotate-issue

## Материалы для issue
### Title
```
Comment: AppendSlice matches #48261
```
### Body
#### Фактура
| | |
|--|--|
| Suite / DB | `ClickbenchParallel1` / `vla_small_column` |
| Branch · Version | `main` · [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100) |
| Run | `2026-07-29_f88e100` · `2026-07-29T12:00:00` UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/12923171727/index.html |
| Failed | Query00
#### Что сломалось
VERIFY; nodata хвост — следствие.
#### К чему приводит
- Abort; cut-off suite.
#### Из‑за чего
crash compaction AppendSlice; nodata — следствие cut-off.
#### Чинить
уже [#48261](https://github.com/ydb-platform/ydb/issues/48261)
#### Детали ошибки
```
VERIFY AppendSlice
```
#### Код
| | |
|--|--|
| Место падения | AppendSlice |
| Связанный issue | [#48261](https://github.com/ydb-platform/ydb/issues/48261) |

<!-- perf-duty-match
kind: olap
fingerprint: AppendSlice
keys:
  - AppendSlice
affected:
  - suite: ClickbenchParallel1
    db: vla_small_column
    queries: [Query00, Query01]
-->
"""
            r = validate_analysis_md(base, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(
                any("nodata after abort" in e for e in r["errors"]),
                r["errors"],
            )
            self.assertTrue(
                any("Query29" in e or "Infrastructure" in e for e in r["errors"]),
                r["errors"],
            )
            self.assertTrue(
                any("ticket_coverage.uncovered_queries" in e for e in r["errors"]),
                r["errors"],
            )
            fixed = base.replace(
                "queries: [Query00, Query01]",
                "queries: [Query00, Query01, Query29, Infrastructure error]",
            )
            r2 = validate_analysis_md(fixed, out_dir=d)
            self.assertTrue(r2["ok"], r2["errors"])

    def test_ok_minimal_olap_fail(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 2}})
            md = """# Perf duty — x

## Заключение
- **Проблема:** VERIFY Groups.end → abort ноды
- **Из‑за чего:** в OnReadResult нет ожидаемой Groups entry
- **Чинить:** уже [#29944](https://github.com/ydb-platform/ydb/issues/29944)
- **Решение:** update_known
- **Виновник:** unknown — known issue; evidence bar на новый PR не пройден
- **Уверенность:** высокая
- **Критичность:** HIGH
- **Давность:** подтверждено на [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100); [#29944](https://github.com/ydb-platform/ydb/issues/29944) с 2025-12

## Проблемы
### P1 — Groups.end
- Тип: olap_fail
- Что сломалось: AFL_VERIFY
- Почему / механика: OnReadResult missing Groups entry
- Логи: kikimr__stderr VERIFY+signal 6; kikimr__logs connection lost after abort
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/tx/columnshard/blobs_action/abstract/read.cpp`
- Кто (если есть): unknown
- Давность: в прошлых прогонах был поверхностный 2005; VERIFY на разбираемом прогоне
- Гипотеза проверена: yes — evidence in stderr
- Связанный issue: [#29944](https://github.com/ydb-platform/ydb/issues/29944)
- Тикет: комментарий в [#29944](https://github.com/ydb-platform/ydb/issues/29944)

## Гипотезы происхождения
- **H1** (подтверждена): в OnReadResult нет ожидаемого Groups entry — writer порции не положил группу до read — [`read.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100ec2eabf78b51b8c09d234484ea1e3958c/ydb/core/tx/columnshard/blobs_action/abstract/read.cpp#L59)
- Issues (поиск): keys `read.cpp:59` — учёл [#29944](https://github.com/ydb-platform/ydb/issues/29944); иных совпадений нет.

## Причины
- Тот же дефект, что [#29944](https://github.com/ydb-platform/ydb/issues/29944); повтор на разбираемом прогоне (давний дефект проявился снова под нагрузкой UploadTpch).

## Как починить
1. Как в [#29944](https://github.com/ydb-platform/ydb/issues/29944): гарантировать наличие Groups до OnReadResult / не ходить в end() без проверки.

## Что дальше
1. Комментарий в [#29944](https://github.com/ydb-platform/ydb/issues/29944)

## Материалы для issue
### Title
```
Comment: UploadTpch VERIFY Groups.end matches #29944
```
### Body
#### Фактура
| | |
|--|--|
| Suite / DB | `UploadTpch100` / `sas_big_column` |
| Branch · Version | `main` · [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100) |
| Run | `2026-07-25_f88e100` · `2026-07-25T12:00:00` UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/12923171727/index.html |
| Failed | Query03 (VERIFY) |
#### Что сломалось
VERIFY Groups.end на разбираемом прогоне — тот же [#29944](https://github.com/ydb-platform/ydb/issues/29944).
#### К чему приводит
- Abort ноды; в Allure — node down / connection lost.
#### Из‑за чего
в OnReadResult нет ожидаемой Groups entry.
#### Чинить
уже [#29944](https://github.com/ydb-platform/ydb/issues/29944)
#### Детали ошибки
```
VERIFY Groups.end
```
#### Код
| | |
|--|--|
| Место падения | [`read.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100ec2eabf78b51b8c09d234484ea1e3958c/ydb/core/tx/columnshard/blobs_action/abstract/read.cpp#L59) |
| Связанный issue | [#29944](https://github.com/ydb-platform/ydb/issues/29944) |

<!-- perf-duty-match
kind: olap
fingerprint: Groups.end
keys:
  - Groups.end
  - read.cpp
affected:
  - suite: UploadTpch100
    db: sas_big_column
    queries: [Query03]
-->
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertTrue(r["ok"], r["errors"])

    def test_rca_rejects_unchanged_path_as_sole_cause(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 1}})
            md = """# Perf duty — x

## Заключение
- **Итог:** VERIFY
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** этот прогон
- **Механика:** VERIFY

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr VERIFY; kikimr__logs empty
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/x.cpp`
- Гипотеза проверена: partial

## Гипотезы происхождения
- **H1** (открыта): assert в read — [`read.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/blobs_action/abstract/read.cpp#L1)
- Issues (поиск): по path read.cpp — совпадений нет.

## Причины
- path из трейса не менялся в окне; bisect unchanged.

## Как починить
1. разобраться дальше

## Что дальше
1. dig

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/1/index.html
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(
                any("path/file unchanged" in e or "не «path" in e or "writers" in e for e in r["errors"]),
                r["errors"],
            )

    def test_rca_rejects_competing_hypotheses(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 1}})
            md = """# Perf duty — x

## Заключение
- **Итог:** VERIFY
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** этот прогон
- **Механика:** VERIFY

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr VERIFY; kikimr__logs empty
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/x.cpp`
- Гипотеза проверена: partial

## Гипотезы происхождения
- **H1** (открыта): dangling key — [`scan.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/counters/scan.cpp#L1)
- **H2** (открыта): map corruption — [`scan.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/counters/scan.cpp#L2)
- Issues (поиск): CountersForStep — учёл [#47284](https://github.com/ydb-platform/ydb/issues/47284).

## Причины
- concurrent conveyor lookup по TStringBuf имени step.

## Как починить
1. owning TString / index-based counters.

## Что дальше
1. dig

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/1/index.html
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(
                any("one most-probable" in e for e in r["errors"]),
                r["errors"],
            )

    def test_rca_requires_hypotheses_causes_fix(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 1}})
            md = """# Perf duty — x

## Заключение
- **Итог:** VERIFY
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** этот прогон
- **Механика:** VERIFY

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr VERIFY; kikimr__logs empty
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/x.cpp`
- Гипотеза проверена: no

## Что дальше
1. dig

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/1/index.html
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            blob = " ".join(r["errors"])
            self.assertIn("Гипотезы происхождения", blob)
            self.assertIn("Причины", blob)
            self.assertIn("Как починить", blob)

    def test_update_known_requires_match_block(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": ["verify"]}})
            write_json(
                d / "code_bisect.json",
                {"introduced_in_window": False, "conclusion": "unchanged"},
            )
            write_json(d / "priors.json", {"prior_scans": []})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 2}})
            md = """# Perf duty — x

## Заключение
- **Итог:** VERIFY Groups.end abort
- **Решение:** update_known
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне
- **Механика:** OnReadResult AFL_VERIFY → SIGABRT

## Проблемы
### P1 — Groups.end
- Тип: olap_fail
- Логи: kikimr__stderr VERIFY; kikimr__logs disconnect
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/x.cpp`
- Гипотеза проверена: yes
- Тикет: [#29944](https://github.com/ydb-platform/ydb/issues/29944)

## Что дальше
1. comment

## Материалы для issue
### Title
```
Comment only
```
### Body
#### Фактура
| | |
|--|--|
| Suite / DB | `UploadTpch100` / `sas_big_column` |
| Branch · Version | `main` · [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100) |
| Run | label · ts UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/12923171727/index.html |
| Failed | Query03 |
#### Что сломалось
x
#### К чему приводит
- x
#### Из‑за чего
x
#### Чинить
здесь
#### Детали ошибки
```
VERIFY
```
#### Код
| | |
|--|--|
| Место падения | x |
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"])
            self.assertTrue(
                any("perf-duty-match" in e for e in r["errors"]),
                r["errors"],
            )

    def test_open_ticket_requires_title_body(self):
        md = """# Perf duty — x

## Заключение
- **Итог:** new abort
- **Решение:** open_ticket
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне
- **Механика:** AFL_VERIFY → SIGABRT

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr VERIFY; kikimr__logs disconnect
- Код ([`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)): `ydb/core/x.cpp`
- Гипотеза проверена: yes

## Что дальше
1. open issue

## Материалы для issue
### Окружение
| Suite | x |
https://proxy.sandbox.yandex-team.ru/12923171727/index.html
https://github.com/ydb-platform/ydb/commit/abc1234
"""
        r = validate_analysis_md(md)
        self.assertFalse(r["ok"])
        self.assertTrue(any("Title" in e and "Body" in e for e in r["errors"]), r["errors"])

    def _open_ticket_sigsegv_md(self, details: str) -> str:
        return f"""# Perf duty — x

## Заключение
- **Итог:** SIGSEGV RemainOnly
- **Решение:** open_ticket
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне
- **Механика:** RemainOnly → signal 11

## Проблемы
### P1 — RemainOnly
- Тип: olap_fail
- Логи: kikimr__stderr signal 11; kikimr__logs cluster dig
- Код ([`d6dd620`](https://github.com/ydb-platform/ydb/commit/d6dd620)): `collection.cpp`
- Гипотеза проверена: yes

## Что дальше
1. Тикет

## Материалы для issue
### Title
```
OLAP: SIGSEGV RemainOnly (collection.cpp:262) on UploadTpch1000
```
### Body
#### Фактура
| | |
|--|--|
| Suite / DB | `UploadTpch1000` / `sas_big_column` |
| Branch · Version | `main` · [`d6dd620`](https://github.com/ydb-platform/ydb/commit/d6dd620) |
| Run | `2026-07-27_d6dd620` · `2026-07-27T08:24:30` UTC |
| Allure | https://proxy.sandbox.yandex-team.ru/12931774388/index.html |
| Failed | Query06 (SIGSEGV) |
#### Что сломалось
SIGSEGV в RemainOnly.
#### К чему приводит
- Crash ноды; node down / connection lost.
#### Из‑за чего
корень ещё не найден; RemainOnly — место падения.
#### Чинить
здесь; ASan / dig writers.
#### Детали ошибки
{details}
#### Код
| | |
|--|--|
| Место падения | [`collection.cpp`](https://github.com/ydb-platform/ydb/blob/d6dd620/ydb/core/formats/arrow/program/collection.cpp#L256) |

<!-- perf-duty-match
kind: olap
fingerprint: collection.cpp:262 RemainOnly
keys:
  - collection.cpp:262
  - RemainOnly
affected:
  - suite: UploadTpch1000
    db: sas_big_column
    queries: [Query06]
-->
"""

    def test_wait_next_wave_requires_s3_report(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(d / "focus.json", {"fetched": True, "fatal": {"signals": []}})
            write_json(d / "code_bisect.json", {"introduced_in_window": False})
            write_json(d / "dig_runs.json", {"kind": "olap", "summary": {"slice_count": 2}})
            write_json(d / "dig_prs.json", {"prs": []})
            write_json(d / "priors.json", {"prior_scans": []})
            md = """# Perf duty — x

## Заключение
- **Проблема:** IC cascade; peer close unknown
- **Из‑за чего:** DeadPeer / YDBE-02001; abort на peer не найден
- **Чинить:** ждать повтор с core на peer
- **Решение:** wait_next_wave
- **Виновник:** unknown
- **Уверенность:** средняя
- **Критичность:** MEDIUM
- **Давность:** на разбираемом прогоне

## Проблемы
### P1 — IC cascade
- Тип: olap_fail
- Что сломалось: Query13 node lost
- Почему / механика: каскад после DeadPeer; причина close peer неизвестна
- Логи: kikimr__stderr empty; kikimr__logs DeadPeer YDBE-02001
- Код ([`c460199`](https://github.com/ydb-platform/ydb/commit/c460199)): n/a
- Кто (если есть): unknown
- Давность: этот прогон
- Гипотеза проверена: partial
- Связанный issue: нет
- Тикет: нет

## Гипотезы происхождения
- **H1** (partial): peer closed IC; нужен abort/core на peer
- Issues (поиск): ExtractBlobsData ticket — fingerprint не совпал

## Причины
- Каскад понятен; root peer close — нет

## Как починить
1. Ждать повтор с evidence на peer

## Что дальше
1. Следующая волна

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/13000000000/index.html
[`c460199`](https://github.com/ydb-platform/ydb/commit/c460199)
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertTrue(
                any("s3_report.json" in e and "wait_next_wave" in e for e in r["errors"]),
                r["errors"],
            )
            write_json(
                d / "s3_report.json",
                {
                    "run_id": "olap-UploadTpch1000-c460199",
                    "stamp": "20260806T000000Z",
                    "analysis_url": (
                        "https://storage.yandexcloud.net/workload-log/"
                        "perfomance_tests_status/duty_artifacts/x/analysis.md"
                    ),
                },
            )
            r2 = validate_analysis_md(md, out_dir=d)
            self.assertFalse(
                any("s3_report.json" in e for e in r2["errors"]),
                r2["errors"],
            )

    def test_open_ticket_rejects_faktura_without_gfm_header(self):
        md = self._open_ticket_sigsegv_md(
            """
Host: `sas9-1593`
Coredump: https://coredumps.yandex-team.ru/v3/cores/abc
```
Received signal 11
Backtrace:
"""
            + "\n".join(f"#{i} frame at x.cpp:{i}" for i in range(12))
            + "\n```\n"
        )
        # Strip GFM header from Фактура — GitHub would not render the table.
        md = md.replace("| | |\n|--|--|\n", "", 1)
        r = validate_analysis_md(md)
        self.assertFalse(r["ok"], r)
        self.assertTrue(
            any("GFM header" in e and "Фактура" in e for e in r["errors"]),
            r["errors"],
        )

    def test_open_ticket_rejects_truncated_backtrace(self):
        details = """
Host: `sas9-1593`
Coredump: https://coredumps.yandex-team.ru/v3/cores?filter=program_type%3Dkikimr
```
Received signal 11
Backtrace:
#5 __yhashtable_iterator<...>::operator++() at .../hash_table.h
#6 RemainOnly at .../collection.cpp:262
#7 DoExecute at .../projection.cpp:9
…
#16 ExecuteTask at .../worker.cpp:19
```
"""
        r = validate_analysis_md(self._open_ticket_sigsegv_md(details))
        self.assertFalse(r["ok"], r)
        blob = " ".join(r["errors"]).lower()
        self.assertTrue(
            "truncat" in blob or "short" in blob or "gap" in blob or "…" in " ".join(r["errors"]),
            r["errors"],
        )

    def test_open_ticket_rejects_coredump_placeholder(self):
        frames = "\n".join(f"#{i} frame_{i} at file.cpp:{i}" for i in range(12))
        details = f"""
Host: `sas9-1593`
Coredump: filter URL в descriptionHtml Allure.
```
Received signal 11
Backtrace:
{frames}
```
"""
        r = validate_analysis_md(self._open_ticket_sigsegv_md(details))
        self.assertFalse(r["ok"], r)
        self.assertTrue(
            any("coredumps.yandex-team.ru" in e or "descriptionHtml" in e for e in r["errors"]),
            r["errors"],
        )

    def test_open_ticket_accepts_full_backtrace_and_coredump_url(self):
        frames = "\n".join(
            f"#{i} NKikimr::Frame{i}() at /-S/ydb/core/x.cpp:{i}:0" for i in range(20)
        )
        details = f"""
Host: `sas9-1593.host.testing.ydb.yandex.net`
Coredump: https://coredumps.yandex-team.ru/v3/cores?filter=program_type%3Dkikimr%3B+%40cluster_name%3Dolap-testing-sas-perf&since_ts=2026-07-27T12%3A01%3A02%2B03%3A00&till_ts=2026-07-27T12%3A01%3A41%2B03%3A00
```
Received signal 11
Backtrace:
{frames}
```
"""
        r = validate_analysis_md(self._open_ticket_sigsegv_md(details))
        self.assertTrue(r["ok"], r["errors"])

    def test_reject_2005_only_olap_fail(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            md = """# Perf duty

## Заключение
- **Итог:** code 2005 cluster unavailable connection with node lost
- **Решение:** wait_next_wave
- **Виновник:** unknown
- **Уверенность:** low
- **Давность:** unknown
- **Механика:** unknown

## Проблемы
### P1 — 2005
- Тип: olap_fail
- Гипотеза проверена: no

## Что дальше
1. wait

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/1/index.html
https://github.com/ydb-platform/ydb/commit/abc1234
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"])
            blob = " ".join(r["errors"])
            self.assertTrue("2005" in blob or "stderr" in blob or "logs" in blob)

    def test_reject_missing_mechanism_since(self):
        md = """# Perf duty

## Заключение
- **Итог:** something
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** low

## Проблемы
### P1 — x
- Гипотеза проверена: no

## Что дальше
1. dig

## Материалы для issue
todo
"""
        r = validate_analysis_md(md)
        self.assertFalse(r["ok"])
        self.assertTrue(any("Механика" in e for e in r["errors"]))
        self.assertTrue(any("Давность" in e or "Since" in e for e in r["errors"]))

    def test_reject_no_action_on_ic_cascade_empty_stderr(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(
                d / "focus.json",
                {
                    "fetched": True,
                    "fatal": {"signals": ["disconnect", "unavailable"]},
                    "allure": {
                        "cases": [
                            {
                                "name": "TpchParallel.Query03",
                                "attach_analysis": {
                                    "signals": ["disconnect"],
                                    "attachments_fetched": [
                                        {"name": "kikimr__stderr"},
                                        {"name": "kikimr__logs"},
                                    ],
                                },
                            }
                        ]
                    },
                },
            )
            write_json(
                d / "code_bisect.json",
                {"conclusion": "bisect skipped", "introduced_in_window": None},
            )
            md = """# Perf duty

## Заключение
- **Итог:** IC DeadPeer → disconnect; следующий прогон FailCount=0
- **Решение:** `no_action`
- **Виновник:** unknown
- **Уверенность:** высокая
- **Давность:** на разбираемом прогоне
- **Механика:** DeadPeer 50001↔50015 → UNAVAILABLE

## Проблемы
### P1 — IC
- Тип: olap_fail
- Логи: kikimr__stderr пустой; kikimr__logs DeadPeer / connection closed by peer
- Код ([`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)): fline нет
- Гипотеза проверена: yes
- Тикет: нет

## Что дальше
1. nothing

## Материалы для issue
Не копировать. https://proxy.sandbox.yandex-team.ru/12923242686/index.html
https://github.com/ydb-platform/ydb/commit/abc1234
"""
            r = validate_analysis_md(md, out_dir=d)
            self.assertFalse(r["ok"], r)
            self.assertTrue(
                any("no_action forbidden" in e for e in r["errors"]),
                r["errors"],
            )


class ResultMergeTests(unittest.TestCase):
    def test_merge_problems(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            ctx = load_context(FIXTURES / "sample_olap.json")
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "id": "P1",
                            "status": "analyzed",
                            "resolution": "update_known",
                            "confidence": "high",
                            "culprit_found": False,
                        }
                    ]
                },
            )
            write_json(d / "detect_type.json", {"analysis_types": ["olap_fail"]})
            out = merge_result(d, ctx=ctx, summary="ok", status="completed")
            self.assertEqual(out["schema"], "perf-duty-result/v1")
            self.assertEqual(out["problems"]["total"], 1)
            self.assertEqual(out["problems"]["analyzed"], 1)
            self.assertEqual(out["resolution"], "update_known")


class SandboxTests(unittest.TestCase):
    def test_disconnected_fingerprint(self):
        html = "<html><body>Error: detected disconnected node at host</body></html>"
        out = extract_fingerprints(html)
        self.assertEqual(out["primary"], "disconnected_node")

    def test_missing_oauth_message(self):
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, {}, clear=False):
            for k in ("SANDBOX_TOKEN", "YA_TOKEN"):
                os.environ.pop(k, None)
            with mock.patch("tools.sandbox.sandbox_oauth_token", return_value=None):
                out = inspect_sandbox(
                    "https://proxy.sandbox.yandex-team.ru/1/index.html",
                    offline=False,
                )
        self.assertFalse(out["fetched"])
        self.assertEqual(out["auth"], "missing")


class YavConfigTests(unittest.TestCase):
    def test_token_config_has_sandbox_and_ydb_sa(self):
        cfg = read_token_config(AGENT / "token_config.json")
        specs = token_specs_from_config(cfg)
        self.assertIn("SANDBOX_TOKEN", specs)
        self.assertEqual(specs["SANDBOX_TOKEN"]["kind"], "string")
        self.assertIn("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", specs)
        sa = specs["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"]
        self.assertEqual(sa["kind"], "file")
        self.assertEqual(sa["key"], "my-robot-key.json")
        self.assertTrue(sa["secret_id"].startswith("sec-"))

    def test_file_kind_materialize(self):
        import tempfile
        from unittest import mock

        from tools import yav as yav_mod

        with tempfile.TemporaryDirectory() as td:
            cache = Path(td) / "cache"
            cfg_path = Path(td) / "token_config.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "tokens": {
                            "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS": {
                                "secret_id": "sec-test",
                                "key": "my-robot-key.json",
                                "kind": "file",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            fake = mock.Mock(
                returncode=0,
                stdout='{"id":"sa","private_key":"x"}\n',
                stderr="",
            )
            with mock.patch.object(yav_mod, "CACHE_DIR", cache):
                with mock.patch.dict(os.environ, {}, clear=False):
                    os.environ.pop("CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", None)
                    os.environ.pop("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", None)
                    with mock.patch("subprocess.run", return_value=fake):
                        tokens = yav_mod.fetch_tokens_from_yav(cfg_path)
            path = Path(tokens["CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"])
            self.assertTrue(path.is_file())
            self.assertIn("private_key", path.read_text(encoding="utf-8"))
            self.assertEqual(
                tokens["YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS"], str(path)
            )


class AttachmentTests(unittest.TestCase):
    def test_scan_log_hosts_nodes(self):
        text = (
            "Connection with node 1234 lost; "
            "host sas-big-column-1.host.testing.ydb.yandex.net down; "
            "5678@vla-foo.host.testing.ydb.yandex.net was restarted"
        )
        out = scan_log_text(text)
        self.assertIn("disconnect", out["signals"])
        self.assertIn("1234", out["nodes"])

    def test_scan_log_signal_11_segfault(self):
        text = (
            "sas9-1578.host.testing.ydb.yandex.net:\n"
            "Received signal 11\n"
            "Backtrace:\n"
            "#6 arrow::io::BufferReader::DoReadAt(long, long, void*)\n"
            "Success. Registered as 50004\n"
        )
        out = scan_log_text(text)
        self.assertIn("segfault", out["signals"])
        self.assertIn("restart", out["signals"])
        self.assertTrue(any("BufferReader" in q for q in out["quotes"]))

    def test_extract_host_dig_hints(self):
        html = (
            "<tr><td>Coredumps</td><td>"
            "<a href='https://coredumps.yandex-team.ru/v3/cores?"
            "filter=program_type%3Dkikimr'>link</a></td></tr>"
            "<tr><td>Kikimr log</td><td><details><code>"
            "parallel-ssh -H sas9-1578.host.testing.ydb.yandex.net -i "
            "'ulimit -n 100500;unified_agent select -S \"2026-07-26T19:23:33+03:00\" "
            "-U \"2026-07-26T19:24:05+03:00\" -s kikimr'"
            "</code></details></td></tr>"
        )
        hints = extract_host_dig_hints(html, log_text="Received signal 11\nBacktrace:\n")
        self.assertTrue(hints["coredump_urls"])
        self.assertTrue(any("unified_agent" in c for c in hints["journal_cmds"]))
        self.assertIn("sas9-1578.host.testing.ydb.yandex.net", hints["hosts"])
        self.assertTrue(hints["local_dump_hint"])

    def test_pick_priority_attachments(self):
        atts = [
            {"name": "noise", "source": "a", "size": 10},
            {"name": "kikimr__stderr", "source": "b", "size": 100},
            {"name": "kikimr__logs", "source": "c.gz", "size": 5000},
        ]
        picked = pick_priority_attachments(atts)
        self.assertEqual([p["name"] for p in picked], ["kikimr__stderr", "kikimr__logs"])

    def test_pick_priority_attachments_with_plans(self):
        atts = [
            {"name": "kikimr__stderr", "source": "b", "size": 100},
            {"name": "Stats", "source": "s.json", "size": 200},
            {"name": "Final plan table", "source": "p0.txt", "size": 300},
            {"name": "Final plan table", "source": "p1.txt", "size": 310},
            {"name": "Plan table", "source": "ex.txt", "size": 250},
        ]
        picked = pick_priority_attachments(atts, include_plans=True)
        names = [p["name"] for p in picked]
        self.assertIn("kikimr__stderr", names)
        self.assertIn("Stats", names)
        self.assertIn("Final plan table", names)
        self.assertIn("Plan table", names)

    def test_summarize_plan_text_hints(self):
        text = "└─ GraceJoin (Left)\n   └─ TableFullScan on lineitem\nTotal duration: 12.5 s"
        out = summarize_plan_text(text, name="Final plan table")
        self.assertIn("grace_join", out["hints"])
        self.assertIn("fullscan", out["hints"])
        self.assertAlmostEqual(out["duration_ms"] or 0, 12500.0, delta=1.0)

    def test_name_matches_slow_query(self):
        self.assertTrue(_name_matches("UploadTpch100.Query03", "Query03"))
        self.assertTrue(_name_matches("UploadTpch100.Query03", "UploadTpch100.Query03"))
        self.assertFalse(_name_matches("UploadTpch100.Query04", "Query03"))


class TraceTests(unittest.TestCase):
    def test_record_and_inject_details(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(d / "detect_type.json", {"analysis_types": ["olap_slow"]})
            write_json(
                d / "dig_runs.json",
                {
                    "summary": {
                        "slice_count": 3,
                        "row_count": 10,
                        "baseline_candidate": {"reason": "min_metric_with_report"},
                        "largest_fail_step": {
                            "from_version": "main.aaa1111",
                            "to_version": "main.bbb2222",
                            "delta": 1,
                        },
                    }
                },
            )
            write_json(
                d / "dig_prs.json",
                {
                    "base": "aaa1111",
                    "head": "bbb2222",
                    "product_prs": [{"pr": 1}],
                    "hot_prs": [{"pr": 1}],
                },
            )
            write_json(
                d / "code_bisect.json",
                {
                    "paths": ["ydb/core/formats/arrow/serializer/native.cpp"],
                    "introduced_in_window": False,
                    "window": {"base": "aaa1111", "head": "bbb2222"},
                },
            )
            (d / "analysis.md").write_text(
                """# Perf duty — t

## Заключение
- **Итог:** x
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** —
- **Механика:** —

## Проблемы
### P1 — x
- Гипотеза проверена: no

## Что дальше
1. more

## Материалы для issue
https://proxy.sandbox.yandex-team.ru/1/index.html
[`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)
""",
                encoding="utf-8",
            )
            trace_record(d, "H1: plan_same", kind="hypothesis", detail="runtime?")
            # Simulate buggy repeated bisect stages (path=None) + dig-prs before rebuild sync
            trace_record(d, "dig-prs", kind="stage", detail="hot=1")
            trace_record(d, "bisect", kind="stage", detail="path=None introduced_in_window=False")
            trace_record(d, "bisect", kind="stage", detail="path=None introduced_in_window=False")
            info = ensure_trace_in_analysis(d, rebuild=True)
            self.assertTrue(info["injected"])
            text = (d / "analysis.md").read_text(encoding="utf-8")
            self.assertIn("duty-action-tree:start", text)
            self.assertIn("<details>", text)
            self.assertIn("Дерево разбора", text)
            self.assertIn("H1: plan_same", text)
            self.assertIn("тип разбора", text)
            self.assertIn("Проверка пути в коде", text)
            self.assertIn("native.cpp", text)
            self.assertIn("не менялся в окне", text)
            self.assertIn("окно aaa1111…bbb2222", text)
            self.assertIn("fail↑", text)
            self.assertNotIn("path=None", text)
            self.assertNotIn("window=..", text)
            # one artifacts rollup only
            self.assertEqual(text.count("Сводка по артефактам"), 1)
            # re-inject replaces block, does not duplicate markers / rollups
            ensure_trace_in_analysis(d, rebuild=True)
            text2 = (d / "analysis.md").read_text(encoding="utf-8")
            self.assertEqual(text2.count("duty-action-tree:start"), 1)
            self.assertEqual(text2.count("Сводка по артефактам"), 1)
            tree = json.loads((d / "action_tree.json").read_text(encoding="utf-8"))
            arts = [n for n in tree["nodes"] if n.get("kind") == "artifacts"]
            self.assertEqual(len(arts), 1)

    def test_render_ascii_nested(self):
        tree = {
            "nodes": [
                {
                    "title": "prepare",
                    "status": "ok",
                    "children": [
                        {"title": "detect_type", "status": "ok", "detail": "olap_slow", "children": []},
                    ],
                }
            ]
        }
        ascii_ = render_ascii_tree(tree)
        self.assertIn("Подготовка", ascii_)
        self.assertIn("тип разбора", ascii_)

    def test_dedupe_root_stages_keeps_latest(self):
        nodes = [
            {"title": "dig-runs", "detail": "old", "children": []},
            {"title": "H1", "kind": "hypothesis", "children": []},
            {"title": "dig-runs", "detail": "new", "children": [{"title": "mart summarize"}]},
            {"title": "dig-prs", "detail": "a", "children": []},
            {"title": "dig-prs", "detail": "b source=stable_streak_end", "children": []},
        ]
        out = _dedupe_root_stages(nodes)
        titles = [n["title"] for n in out]
        self.assertEqual(titles.count("dig-runs"), 1)
        self.assertEqual(titles.count("dig-prs"), 1)
        self.assertEqual(out[titles.index("dig-runs")]["detail"], "new")
        self.assertIn("stable_streak", out[titles.index("dig-prs")]["detail"])
        self.assertIn("H1", titles)


class BaselineTests(unittest.TestCase):
    def test_select_baseline_prefers_jump_from(self):
        runs = [
            {"RunTs": "t1", "Version": "aaa", "YdbSumMeans": 100, "Report": "https://proxy/1/"},
            {"RunTs": "t2", "Version": "bbb", "YdbSumMeans": 110, "Report": "https://proxy/2/"},
            {"RunTs": "t3", "Version": "ccc", "YdbSumMeans": 200, "Report": "https://proxy/3/"},
        ]
        jump = {"from_ts": "t2", "to_ts": "t3", "from": 110, "to": 200, "delta": 90}
        b = select_baseline_from_slice_runs(runs, metric="YdbSumMeans", jump=jump)
        self.assertIsNotNone(b)
        self.assertEqual(b["reason"], "largest_step_from")
        self.assertEqual(b["Version"], "bbb")
        self.assertEqual(b["Report"], "https://proxy/2/")

    def test_select_baseline_from_pack_history(self):
        ctx = {
            "report": {"kind": "olap"},
            "suite_history": {
                "labels": ["a", "b", "c"],
                "versions": ["111", "222", "333"],
                "ydb": [50, 55, 120],
                "reports": [
                    "https://proxy/a/",
                    "https://proxy/b/",
                    "https://proxy/c/",
                ],
            },
        }
        b = select_baseline_from_pack_history(ctx)
        self.assertIsNotNone(b)
        self.assertTrue(b["Report"])
        self.assertLess(b["metric_value"], 120)

    def test_compare_plan_digs(self):
        focus = [
            {
                "name": "Suite.Query01",
                "attach_analysis": {
                    "plan_dig": {"hints": ["grace_join", "fullscan"], "iterations": [{}]}
                },
            }
        ]
        base = [
            {
                "name": "Suite.Query01",
                "attach_analysis": {"plan_dig": {"hints": ["lookup"], "iterations": [{}]}},
            }
        ]
        cmp_ = compare_plan_digs(focus, base)
        self.assertEqual(cmp_["comparisons"][0]["verdict"], "plan_regressed")


class DutyctlCliTests(unittest.TestCase):
    def test_prepare_cli_offline(self):
        run = AGENT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(run),
                    "prepare",
                    "-c",
                    str(FIXTURES / "sample_olap.json"),
                    "-o",
                    str(out),
                    "--offline",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            det = json.loads((out / "detect_type.json").read_text(encoding="utf-8"))
            self.assertIn("olap_fail", det["analysis_types"])
            self.assertTrue((out / "focus.json").is_file())
            self.assertTrue((out / "priors.json").is_file())
            self.assertTrue((out / "result.json").is_file())
            focus = json.loads((out / "focus.json").read_text(encoding="utf-8"))
            self.assertIn("fatal", focus)

    def test_prepare_tpcc_metrics(self):
        run = AGENT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(run),
                    "prepare",
                    "-c",
                    str(FIXTURES / "sample_tpcc.json"),
                    "-o",
                    str(out),
                    "--offline",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            self.assertTrue((out / "metrics_delta.json").is_file())

    def test_validate_cli_ok(self):
        run = AGENT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            (out / "analysis.md").write_text(
                """# Perf duty — t

## Заключение
- **Проблема:** fail, стека пока нет
- **Из‑за чего:** корень ещё не найден
- **Чинить:** здесь; нужны stderr/logs
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Критичность:** MEDIUM
- **Давность:** неизвестно — нужны прошлые прогоны

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr empty; kikimr__logs empty
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/tx/columnshard/x.cpp`
- Гипотеза проверена: no

## Гипотезы происхождения
- **H1** (открыта): порча раньше детекции — смотреть writers вокруг CS read — [`read.cpp`](https://github.com/ydb-platform/ydb/blob/f88e100aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/ydb/core/tx/columnshard/blobs_action/abstract/read.cpp#L1)
- Issues (поиск): по `read.cpp` / Groups — соседних open нет.

## Причины
- Пока без suspect PR; нужен полный stderr — дефект может проявляться только под нагрузкой suite.

## Как починить
1. Скачать kikimr__stderr/logs и сузить H1 по стеку writers.

## Что дальше
1. скачать больше логов

## Материалы для issue
### Отчёты
https://proxy.sandbox.yandex-team.ru/12923171727/index.html
### Код
https://github.com/ydb-platform/ydb/commit/f88e100
""",
                encoding="utf-8",
            )
            write_json(out / "detect_type.json", {"analysis_types": ["olap_fail"]})
            write_json(out / "focus.json", {"fetched": False, "fatal": {}})
            write_json(out / "code_bisect.json", {"introduced_in_window": None})
            write_json(out / "dig_runs.json", {"kind": "olap", "summary": {}})
            proc = subprocess.run(
                [sys.executable, str(run), "validate", "-o", str(out)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)


class S3UploadHelpersTests(unittest.TestCase):
    def test_content_type_and_public_url(self):
        from tools.s3_upload import content_type_for, public_url

        self.assertTrue(content_type_for(Path("analysis.md")).startswith("text/markdown"))
        self.assertEqual(content_type_for(Path("result.json")), "application/json")
        url = public_url(
            "workload-log",
            "perfomance_tests_status/duty_artifacts/run/stamp/analysis.md",
        )
        self.assertIn("workload-log", url)
        self.assertTrue(url.endswith("analysis.md"))

    def test_wait_next_queries_fallback_uncovered(self):
        """Suite wipe: only Infrastructure error in problems → use pack uncovered."""
        from tools.s3_upload import _queries_for_wait_next

        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "context.json",
                {
                    "report": {"kind": "olap"},
                    "selection": {
                        "branch": "main",
                        "db": "sas_small_column",
                        "suite": "Tpcds10",
                        "focus_run": {
                            "label": "2026-08-09_1137c6b",
                            "uncovered_queries": [
                                "Infrastructure error",
                                "Query01",
                                "Query16",
                            ],
                        },
                    },
                },
            )
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "test": "Infrastructure error",
                            "resolution": "wait_next_wave",
                        },
                        {"resolution": "wait_next_wave", "summary": "nodata tail"},
                    ]
                },
            )
            qs = _queries_for_wait_next(d)
            self.assertIn("Infrastructure error", qs)
            self.assertIn("Query01", qs)
            self.assertIn("Query16", qs)

    def test_build_wait_next_wave_decision_and_index_merge(self):
        from common.duty_decisions import (
            empty_index,
            focus_key,
            merge_decision_into_index,
        )
        from tools.s3_upload import build_wait_next_wave_decision

        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            write_json(
                d / "context.json",
                {
                    "report": {"kind": "olap"},
                    "selection": {
                        "branch": "stable-26-3-1",
                        "db": "sas_small_column",
                        "suite": "UploadTpch1000",
                        "focus_run": {"label": "2026-08-05_c460199", "sha": "c460199"},
                    },
                },
            )
            write_json(
                d / "result.json",
                {"resolution": "wait_next_wave", "summary": "IC cascade; wait peer abort"},
            )
            write_json(
                d / "problems.json",
                {
                    "items": [
                        {
                            "test": "Query18",
                            "resolution": "wait_next_wave",
                            "summary": "peer abort missing",
                        },
                        {
                            "test": "Query12",
                            "resolution": "update_known",
                            "related_issue": 49182,
                        },
                    ]
                },
            )
            meta = {
                "run_id": "olap-UploadTpch1000-c460199",
                "stamp": "20260806T135942Z",
                "analysis_url": "https://example/analysis.md",
                "files": [
                    {"file": "analysis.md", "url": "https://example/analysis.md"},
                    {"file": "result.json", "url": "https://example/result.json"},
                ],
            }
            dec = build_wait_next_wave_decision(d, meta)
            self.assertIsNotNone(dec)
            assert dec is not None
            self.assertEqual(dec["resolution"], "wait_next_wave")
            self.assertEqual(dec["label"], "2026-08-05_c460199")
            self.assertEqual(dec["queries"], ["Query18"])
            self.assertEqual(
                dec["focus_key"],
                focus_key(
                    kind="olap",
                    branch="stable-26-3-1",
                    db="sas_small_column",
                    suite="UploadTpch1000",
                    label="2026-08-05_c460199",
                ),
            )
            self.assertIn("by_focus/olap/stable-26-3-1/", dec["pointer_key"])
            idx = merge_decision_into_index(empty_index(), dec, updated_at="t0")
            self.assertIn(dec["focus_key"], idx["items"])
            self.assertEqual(idx["items"][dec["focus_key"]]["analysis_url"], meta["analysis_url"])
            # second merge overwrites same key
            dec2 = {**dec, "analysis_url": "https://example/v2.md", "stamp": "later"}
            idx2 = merge_decision_into_index(idx, dec2, updated_at="t1")
            self.assertEqual(len(idx2["items"]), 1)
            self.assertEqual(idx2["items"][dec["focus_key"]]["analysis_url"], "https://example/v2.md")

    def test_human_links_body_upsert_and_issue_detect(self):
        from tools.s3_upload import (
            detect_issue_number,
            format_duty_report_links,
            has_human_duty_report_links,
            upsert_duty_report_in_body,
        )

        files = [
            {"file": "analysis.md", "url": "https://example/a.md"},
            {"file": "result.json", "url": "https://example/r.json"},
            {"file": "problems.json", "url": "https://example/p.json"},
        ]
        links = format_duty_report_links(files)
        self.assertEqual(
            links,
            "[полный отчёт](https://example/a.md) · "
            "[result](https://example/r.json) · "
            "[problems](https://example/p.json)",
        )
        body = (
            "| Allure | https://proxy.example/1 |\n"
            "| Failed | Query06 |\n"
        )
        out = upsert_duty_report_in_body(body, files)
        self.assertIn("| Duty report | [полный отчёт](https://example/a.md)", out)
        self.assertTrue(has_human_duty_report_links(out))
        self.assertLess(out.index("Duty report"), out.index("Failed"))
        body2 = (
            "| Allure | https://proxy.example/1 |\n"
            "| Duty report | [https://old/long](https://old/long) |\n"
            "| Failed | Query06 |\n"
        )
        out2 = upsert_duty_report_in_body(body2, files)
        self.assertEqual(out2.count("Duty report"), 1)
        self.assertIn("[полный отчёт](https://example/a.md)", out2)
        self.assertNotIn("https://old/long", out2)

        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "analysis.md").write_text(
                "Тикет: [#48256](https://github.com/ydb-platform/ydb/issues/48256)\n",
                encoding="utf-8",
            )
            self.assertEqual(detect_issue_number(d), 48256)


class KnownIssuesRelatedClosedTests(unittest.TestCase):
    def test_search_keys_splits_open_and_related_closed(self):
        from tools import known_issues as ki

        fake = [
            {
                "number": 10,
                "title": "open hit",
                "state": "open",
                "keys": ["blob_cache.cpp:468", "TBlobCache::SendResult"],
                "fingerprint": "blob_cache.cpp:468",
                "url": "https://github.com/ydb-platform/ydb/issues/10",
                "affected": [],
            },
            {
                "number": 47872,
                "title": "closed same keys",
                "state": "closed",
                "closed_at": "2026-07-28T14:18:04Z",
                "keys": ["blob_cache.cpp:468", "TBlobCache::SendResult"],
                "fingerprint": "blob_cache.cpp:468",
                "url": "https://github.com/ydb-platform/ydb/issues/47872",
                "affected": [],
            },
            {
                "number": 99,
                "title": "other fingerprint",
                "state": "closed",
                "keys": ["scan.cpp:194"],
                "fingerprint": "scan.cpp:194",
                "url": "https://github.com/ydb-platform/ydb/issues/99",
                "affected": [],
            },
        ]

        def _fake_fetch(**_kwargs):
            return fake, None

        old = ki.fetch_duty_issues
        ki.fetch_duty_issues = _fake_fetch  # type: ignore[assignment]
        try:
            out = ki.search_keys_with_related(
                ["blob_cache.cpp:468", "TBlobCache::SendResult"]
            )
        finally:
            ki.fetch_duty_issues = old  # type: ignore[assignment]

        self.assertEqual([h["number"] for h in out["open_hits"]], [10])
        self.assertEqual([h["number"] for h in out["hits"]], [10])
        self.assertEqual([h["number"] for h in out["related_closed"]], [47872])
        self.assertEqual(out["keys"][0], "blob_cache.cpp:468")

    def test_validate_open_ticket_requires_related_closed_links(self):
        from tools.validate_report import validate_analysis_md

        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "detect_type.json").write_text(
                json.dumps({"analysis_types": ["olap_fail"], "rollup": "olap_fail"}),
                encoding="utf-8",
            )
            (d / "known_issues.json").write_text(
                json.dumps(
                    {
                        "keys": ["blob_cache.cpp:468"],
                        "open_hits": [],
                        "hits": [],
                        "related_closed": [
                            {
                                "number": 47872,
                                "title": "old",
                                "state": "closed",
                                "url": "https://github.com/ydb-platform/ydb/issues/47872",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            # Minimal analysis that reaches the related_closed gate
            md = """# Perf duty — t

## Заключение
- **Проблема:** crash
- **Из‑за чего:** место падения
- **Чинить:** здесь
- **Решение:** `open_ticket`
- **Виновник:** unknown
- **Уверенность:** средняя
- **Давность:** этот прогон
- **Механика:** SIGSEGV

## Гипотезы происхождения
- **H1** (наиболее вероятна): x — [`blob_cache.cpp`](https://github.com/ydb-platform/ydb/blob/abc1234/ydb/core/tx/columnshard/blob_cache.cpp)
- Issues (поиск): known-issues — открытых нет

## Причины
- suspect PR неизвестен

## Как починить
1. ASan

## Проблемы
### P1 — crash
- Тип: `olap_fail`
- Что сломалось: Query06
- Почему / механика: SIGSEGV
- Логи: kikimr__stderr dig; kikimr__logs cascade
- Код ([`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234)): blob_cache
- Кто: unknown
- Гипотеза проверена: partial

## Что дальше
1. тикет

## Материалы для issue
### Title
```
OLAP: SIGSEGV blob_cache.cpp:468
```
### Body
#### Фактура

| | |
|--|--|
| Suite / DB | UploadTpch100 / sas_small_column |
| Branch · Version | main · [`abc1234`](https://github.com/ydb-platform/ydb/commit/abc1234) |
| Run | label · ts |
| Allure | https://proxy.sandbox.yandex-team.ru/1/index.html |
| Failed | Query06 |

#### Что сломалось
crash

#### К чему приводит
- node down

#### Из‑за чего
место падения

#### Чинить
здесь

#### Детали ошибки
stderr empty

#### Код

| | |
|--|--|
| Место падения | blob_cache |
| Связанный issue | нет |
"""
            (d / "analysis.md").write_text(md, encoding="utf-8")
            report = validate_analysis_md(md, out_dir=d)
            joined = " ".join(report.get("errors") or [])
            self.assertIn("related_closed", joined)
            self.assertIn("#47872", joined)

            md2 = md.replace(
                "| Failed | Query06 |",
                "| Failed | Query06 |\n"
                "| Related closed | [#47872](https://github.com/ydb-platform/ydb/issues/47872) |",
            ).replace(
                "#### Чинить\nздесь",
                "#### Чинить\nздесь; заодно [#47872](https://github.com/ydb-platform/ydb/issues/47872)",
            )
            (d / "analysis.md").write_text(md2, encoding="utf-8")
            report2 = validate_analysis_md(md2, out_dir=d)
            joined2 = " ".join(report2.get("errors") or [])
            self.assertNotIn("related_closed must be linked", joined2)


class SightingCommentTests(unittest.TestCase):
    def test_format_sighting_has_links(self):
        from tools.known_issues import format_sighting_comment

        md = format_sighting_comment(
            suite="UploadTpch1000",
            db="vla_small_column",
            queries=["Query05", "Query06"],
            branch="main",
            sha="60199b53fb8af019f16d944ccb529784d993e01f",
            label="2026-07-29_60199b5",
            ts="2026-07-29T12:20:36",
            allure_url="https://proxy.sandbox.yandex-team.ru/12968802365/index.html",
            duty_report_md="[полный отчёт](https://example/analysis.md)",
        )
        self.assertIn("### Повтор", md)
        self.assertIn("github.com/ydb-platform/ydb/commit/60199b53fb8af019f16d944ccb529784d993e01f", md)
        self.assertIn("`60199b5`", md)
        self.assertIn("proxy.sandbox.yandex-team.ru/12968802365", md)
        self.assertIn("полный отчёт", md)
        self.assertNotIn("also seen", md)

    def test_sighting_from_run_reads_context(self):
        from tools.known_issues import sighting_comment_from_run

        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            (d / "context.json").write_text(
                json.dumps(
                    {
                        "selection": {
                            "branch": "main",
                            "db": "vla_small_column",
                            "suite": "UploadTpch1000",
                            "focus_run": {
                                "label": "2026-07-29_60199b5",
                                "sha": "60199b5",
                                "ts": "2026-07-29T12:20:36",
                                "report": "https://example/allure",
                            },
                        },
                        "ticket_coverage": {
                            "uncovered_queries": ["Query05", "Query06"]
                        },
                    }
                ),
                encoding="utf-8",
            )
            (d / "analysis.md").write_text(
                "commit 60199b53fb8af019f16d944ccb529784d993e01f\n",
                encoding="utf-8",
            )
            (d / "s3_report.json").write_text(
                json.dumps({"links_md": "[полный отчёт](https://example/a.md)"}),
                encoding="utf-8",
            )
            md = sighting_comment_from_run(d)
            self.assertIn("`Query05`", md)
            self.assertIn("60199b53fb8af019f16d944ccb529784d993e01f", md)
            self.assertIn("https://example/allure", md)
            self.assertIn("https://example/a.md", md)


class ZipBundleTests(unittest.TestCase):
    def test_load_zip_and_local_sandbox(self):
        with tempfile.TemporaryDirectory() as td:
            ctx = json.loads((FIXTURES / "sample_olap.json").read_text(encoding="utf-8"))
            fr = ctx["selection"]["focus_run"]
            fr["report"] = "https://example.test/sandbox/index.html"
            fr["report_local"] = "sandbox/focus/index.html"
            zip_path = Path(td) / "pack.zip"
            with zipfile.ZipFile(zip_path, "w") as zf:
                zf.writestr("context.json", json.dumps(ctx))
                html = "<html><body>Error: detected disconnected node xyz</body></html>"
                zf.writestr("sandbox/focus/index.html", html)
            loaded = load_context_pack(zip_path)
            try:
                local = focus_report_local(loaded.ctx, loaded.base_dir)
                self.assertIsNotNone(local)
                sb = inspect_sandbox(None, local_path=local, offline=True)
                self.assertTrue(sb["fetched"])
                self.assertEqual(sb["primary"], "disconnected_node")
            finally:
                loaded.close()


if __name__ == "__main__":
    unittest.main()
