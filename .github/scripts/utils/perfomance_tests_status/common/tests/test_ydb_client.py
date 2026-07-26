#!/usr/bin/env python3
"""Unit tests for common.ydb_client (no live YDB)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

PTS = Path(__file__).resolve().parents[1].parent
sys.path.insert(0, str(PTS))

from common.ydb_client import (  # noqa: E402
    analytics_path,
    jsonable,
    to_result_sets,
)


class YdbClientUnitTests(unittest.TestCase):
    def test_analytics_path_exists(self):
        p = analytics_path()
        self.assertTrue(p.name == "analytics")
        self.assertTrue((p / "ydb_wrapper.py").is_file())

    def test_to_result_sets(self):
        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        payload = to_result_sets(rows)
        rs = payload["result_sets"][0]
        self.assertEqual(rs["columns"], ["a", "b"])
        self.assertEqual(rs["rows"], [[1, "x"], [2, "y"]])

    def test_jsonable_decimal_bytes(self):
        from decimal import Decimal

        self.assertEqual(jsonable(Decimal("1.5")), 1.5)
        self.assertEqual(jsonable(b"hi"), "hi")


if __name__ == "__main__":
    unittest.main()
