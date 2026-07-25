#!/usr/bin/env python3
"""Unit tests for duty_agent (olap + tpcc contexts)."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from tools.card import build_card_payload, render_markdown  # noqa: E402
from tools.classify import prelabel  # noqa: E402
from tools.context import ContextError, load_context, validate_context  # noqa: E402
from tools.history import analyze_history, fail_first_seen  # noqa: E402
from tools.sandbox import extract_fingerprints  # noqa: E402


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

    def test_reject_missing_suite(self):
        with self.assertRaises(ContextError):
            validate_context(
                {
                    "schema": "perf-duty-context/v1",
                    "report": {"kind": "olap"},
                    "selection": {"branch": "main", "db": "x"},
                }
            )


class SandboxTests(unittest.TestCase):
    def test_disconnected_fingerprint(self):
        html = "<html><body>Error: detected disconnected node at host</body></html>"
        out = extract_fingerprints(html)
        self.assertEqual(out["primary"], "disconnected_node")
        self.assertIn("disconnected_node", out["fingerprints"])
        self.assertTrue(out["quotes"])


class HistoryTests(unittest.TestCase):
    def test_fail_first_seen(self):
        hist = {
            "labels": ["a", "b", "c"],
            "fail_rate": [0.0, 0.2, 0.5],
            "versions": ["1", "2", "3"],
        }
        out = fail_first_seen(hist)
        self.assertEqual(out["first_label"], "b")
        self.assertEqual(out["first_version"], "2")

    def test_analyze_both_kinds(self):
        olap = load_context(ROOT / "fixtures" / "sample_olap.json")
        tpcc = load_context(ROOT / "fixtures" / "sample_tpcc.json")
        ho = analyze_history(olap)
        ht = analyze_history(tpcc)
        self.assertEqual(ho["kind"], "olap")
        self.assertTrue(ho["suite"]["chronic_in_window"])
        self.assertEqual(ht["kind"], "tpcc")
        self.assertIn("lat90", ht["suite"])


class ClassifyTests(unittest.TestCase):
    def test_olap_chronic(self):
        ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
        hist = analyze_history(ctx)
        lab = prelabel(ctx, {"fingerprints": [], "primary": None}, hist)
        self.assertIn("chronic_fail", lab["labels"])

    def test_olap_infra_from_sandbox(self):
        ctx = load_context(ROOT / "fixtures" / "sample_olap.json")
        hist = analyze_history(ctx)
        lab = prelabel(
            ctx,
            {"fingerprints": ["disconnected_node"], "primary": "disconnected_node"},
            hist,
        )
        self.assertIn("infra", lab["labels"])
        self.assertTrue(str(lab["hypothesis"]).startswith("infra_event"))

    def test_tpcc_regression(self):
        ctx = load_context(ROOT / "fixtures" / "sample_tpcc.json")
        hist = analyze_history(ctx)
        lab = prelabel(ctx, {"fingerprints": [], "primary": None}, hist)
        self.assertTrue(
            "lat_regression" in lab["labels"] or "tpmc_regression" in lab["labels"]
        )


class CardAndCliTests(unittest.TestCase):
    def test_card_markdown_both(self):
        for name in ("sample_olap.json", "sample_tpcc.json"):
            ctx = load_context(ROOT / "fixtures" / name)
            hist = analyze_history(ctx)
            lab = prelabel(ctx, {"fingerprints": [], "primary": None, "fetched": False}, hist)
            card = build_card_payload(
                ctx,
                sandbox={"url": None, "fetched": False, "fingerprints": [], "quotes": []},
                history=hist,
                label=lab,
            )
            md = render_markdown(card)
            self.assertIn("Duty card", md)
            self.assertIn(ctx["selection"]["suite"], md)

    def test_cli_offline_both(self):
        run = ROOT / "run.py"
        for name in ("sample_olap.json", "sample_tpcc.json"):
            with tempfile.TemporaryDirectory() as td:
                out = Path(td) / "card.md"
                proc = subprocess.run(
                    [
                        sys.executable,
                        str(run),
                        "--context",
                        str(ROOT / "fixtures" / name),
                        "--out",
                        str(out),
                        "--offline",
                        "--json",
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
                self.assertTrue(out.is_file())
                self.assertTrue(out.with_suffix(".json").is_file())
                data = json.loads(out.with_suffix(".json").read_text(encoding="utf-8"))
                self.assertEqual(data["schema"], "perf-duty-card/v1")


if __name__ == "__main__":
    unittest.main()
