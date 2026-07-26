#!/usr/bin/env python3
"""Unit tests for dutyctl toolbox (facts + validate; no autopilot RCA)."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from tools.attachments import pick_priority_attachments, scan_log_text  # noqa: E402
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
        ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
        self.assertEqual(ctx["report"]["kind"], "olap")

    def test_load_tpcc(self):
        ctx = load_context(ROOT / "fixtures" / "sample_tpcc.json")
        self.assertEqual(ctx["report"]["kind"], "tpcc")

    def test_reject_bad_schema(self):
        with self.assertRaises(ContextError):
            validate_context({"schema": "nope", "report": {"kind": "olap"}, "selection": {}})


class DetectTypeTests(unittest.TestCase):
    def test_olap_fail_seed(self):
        ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
        det = detect_type(ctx)
        self.assertEqual(det["rollup"], "olap_fail")
        self.assertIn("olap_fail", det["analysis_types"])
        self.assertTrue(any(p.get("test") == "Query05" for p in det["problems_seed"]))

    def test_tpcc_mixed_lat_tpmc(self):
        ctx = load_context(ROOT / "fixtures" / "sample_tpcc.json")
        det = detect_type(ctx)
        self.assertEqual(det["rollup"], "mixed")
        self.assertIn("tpcc_tpmc", det["analysis_types"])
        self.assertIn("tpcc_lat", det["analysis_types"])


class MetricsDeltaTests(unittest.TestCase):
    def test_tpcc_flags(self):
        ctx = load_context(ROOT / "fixtures" / "sample_tpcc.json")
        m = metrics_delta(ctx)
        self.assertIn("lat_regression", m["flags"])
        self.assertIn("tpmc_regression", m["flags"])


class DigRunsTests(unittest.TestCase):
    def test_build_tpcc_sql_and_summarize(self):
        ctx = load_context(ROOT / "fixtures" / "sample_tpcc.json")
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
        ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
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


class ValidateTests(unittest.TestCase):
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
- **Итог:** VERIFY Groups.end abort
- **Решение:** update_known
- **Виновник:** unknown — bisect path unchanged; known issue
- **Уверенность:** высокая
- **Давность:** первый fail на [`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100); [#29944](https://github.com/ydb-platform/ydb/issues/29944) с 2025-12
- **Механика:** OnReadResult AFL_VERIFY → SIGABRT → соседи видят 2005

## Проблемы
### P1 — Groups.end
- Тип: olap_fail
- Что сломалось: AFL_VERIFY
- Почему / механика: OnReadResult missing Groups entry
- Логи: kikimr__stderr VERIFY+signal 6; kikimr__logs connection lost after abort
- Код ([`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)): `ydb/core/tx/columnshard/blobs_action/abstract/read.cpp`
- Кто (если есть): unknown — evidence bisect unchanged
- Давность: в прошлых прогонах был поверхностный 2005; VERIFY на разбираемом прогоне
- Гипотеза проверена: yes — evidence in stderr
- Связанный issue: [#29944](https://github.com/ydb-platform/ydb/issues/29944)

## Что дальше
1. Комментарий в [#29944](https://github.com/ydb-platform/ydb/issues/29944)

## Материалы для issue
### Окружение
| Suite | x |
### Отчёты Sandbox / Allure
https://proxy.sandbox.yandex-team.ru/12923171727/index.html
### Код
[`f88e100`](https://github.com/ydb-platform/ydb/commit/f88e100)
### Доказательства из логов
VERIFY Groups.end
### Что важно для формулировки issue
1. Корневая причина = VERIFY
"""
            r = validate_analysis_md(md, out_dir=d)
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


class ResultMergeTests(unittest.TestCase):
    def test_merge_problems(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
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
    def test_token_config_has_sandbox(self):
        cfg = read_token_config(ROOT / "token_config.json")
        specs = token_specs_from_config(cfg)
        self.assertIn("SANDBOX_TOKEN", specs)


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

    def test_pick_priority_attachments(self):
        atts = [
            {"name": "noise", "source": "a", "size": 10},
            {"name": "kikimr__stderr", "source": "b", "size": 100},
            {"name": "kikimr__logs", "source": "c.gz", "size": 5000},
        ]
        picked = pick_priority_attachments(atts)
        self.assertEqual([p["name"] for p in picked], ["kikimr__stderr", "kikimr__logs"])


class DutyctlCliTests(unittest.TestCase):
    def test_prepare_cli_offline(self):
        run = ROOT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(run),
                    "prepare",
                    "-c",
                    str(ROOT / "fixtures" / "sample_olap.json"),
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
        run = ROOT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            proc = subprocess.run(
                [
                    sys.executable,
                    str(run),
                    "prepare",
                    "-c",
                    str(ROOT / "fixtures" / "sample_tpcc.json"),
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
        run = ROOT / "dutyctl.py"
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)
            (out / "analysis.md").write_text(
                """# Perf duty — t

## Заключение
- **Итог:** test
- **Решение:** investigate_further
- **Виновник:** unknown
- **Уверенность:** низкая
- **Давность:** неизвестно — нужны прошлые прогоны
- **Механика:** unclear; stderr empty so far

## Проблемы
### P1 — x
- Тип: olap_fail
- Логи: kikimr__stderr empty; kikimr__logs empty
- Код (sha неизвестен): путь в `ydb/core/` пока не найден
- Гипотеза проверена: no

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


class ZipBundleTests(unittest.TestCase):
    def test_load_zip_and_local_sandbox(self):
        with tempfile.TemporaryDirectory() as td:
            ctx = json.loads((ROOT / "fixtures" / "sample_olap.json").read_text(encoding="utf-8"))
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
