#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "analytics" / "github_actions"))

from ya_evlog_phases import phases_from_events


def _node(name: str, start: float, end: float) -> dict:
    return {
        "namespace": "worker_threads",
        "event": "node-finished",
        "value": {"name": name, "time": [start, end]},
    }


class EvlogPhasesTest(unittest.TestCase):
    def test_build_before_tests_and_rebuild_after(self):
        events = [
            _node("Compile(a.cpp)", 10, 40),
            _node("Link(lib.a)", 30, 50),
            _node("FromDistCache(hash)", 12, 13),
            _node("Run(test_one)", 60, 90),
            _node("Run(test_two)", 70, 100),
            _node("Compile(b.cpp)", 120, 200),
            _node("Compile(c.cpp)", 180, 210),
            _node("Run(test_three)", 400, 460),
        ]
        self.assertEqual(
            phases_from_events(events, gap=15),
            [
                ("ya_build", 10, 50),
                ("ya_tests", 60, 100),
                ("ya_rebuild", 120, 210),
                ("ya_tests", 400, 460),
            ],
        )

    def test_object_run_after_tests_is_rebuild(self):
        events = [
            _node("Run(rnd-aaa$(BUILD_ROOT)/ydb/lib/ut/test-results/unittest)", 10, 40),
            _node(
                "Run(hash$(BUILD_ROOT)/ydb/core/hive/tx.cpp.o)",
                50,
                200,
            ),
            _node("Run(hash$(BUILD_ROOT)/ydb/core/hive/lib.a)", 180, 220),
            _node("PutInDistCache(hash)", 210, 211),
            _node("Run(rnd-bbb$(BUILD_ROOT)/ydb/lib/ut/test-results/unittest)", 400, 460),
        ]
        self.assertEqual(
            phases_from_events(events, gap=15),
            [
                ("ya_tests", 10, 40),
                ("ya_rebuild", 50, 220),
                ("ya_tests", 400, 460),
            ],
        )

    def test_cache_hit_has_tests_only(self):
        events = [
            _node("FromDistCache(hash)", 1, 2),
            _node("Run(test_one)", 5, 20),
        ]
        self.assertEqual(phases_from_events(events), [("ya_tests", 5, 20)])


if __name__ == "__main__":
    unittest.main()
