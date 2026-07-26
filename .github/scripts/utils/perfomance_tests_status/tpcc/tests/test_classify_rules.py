#!/usr/bin/env python3
"""Unit tests for TPC-C compare-delta rules (mirror of template.html; no YDB / no HTML)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classify_rules import compare_delta_tpcc  # noqa: E402


class CompareDeltaTpccTests(unittest.TestCase):
    def test_mixed_lat_worse_tpmc_better(self):
        prev = {
            "status": "lat",
            "n_lat": 1,
            "n_tpmc": 1,
            "n_broken": 0,
            "lat_pct": 12.0,
            "tpmc_pct": -15.0,
        }
        now = {
            "status": "both",
            "n_lat": 1,
            "n_tpmc": 1,
            "n_broken": 0,
            "lat_pct": 25.0,  # lat worse by +13pp
            "tpmc_pct": -2.0,  # tpmC recovered (less negative) → better by 13pp
        }
        self.assertEqual(compare_delta_tpcc(prev, now), "mixed")

    def test_worse_hot_lat_step(self):
        prev = {
            "status": "lat",
            "n_lat": 1,
            "n_tpmc": 0,
            "n_broken": 0,
            "lat_pct": 12.0,
            "tpmc_pct": 0.0,
        }
        now = {
            "status": "lat",
            "n_lat": 1,
            "n_tpmc": 0,
            "n_broken": 0,
            "lat_pct": 25.0,
            "tpmc_pct": 0.0,
        }
        self.assertEqual(compare_delta_tpcc(prev, now), "worse-hot")


if __name__ == "__main__":
    unittest.main()
