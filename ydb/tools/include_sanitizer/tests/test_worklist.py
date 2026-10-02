"""Tests for the fused optimization worklist."""

from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.report.worklist import (  # noqa: E402
    build_worklist,
    is_editable,
    is_generated,
    load_costs,
    load_graph,
    to_rel,
)


class HelpersTest(unittest.TestCase):
    def test_to_rel(self) -> None:
        self.assertEqual(to_rel("/home/x/work/ydb/core/base/blobstorage.h"),
                         "ydb/core/base/blobstorage.h")
        self.assertEqual(to_rel("/a/b/contrib/libs/protobuf/x.pb.h"),
                         "contrib/libs/protobuf/x.pb.h")
        self.assertEqual(to_rel("ydb/core/x.h"), "ydb/core/x.h")  # already rel

    def test_classifiers(self) -> None:
        self.assertTrue(is_editable("ydb/core/x.h"))
        self.assertFalse(is_editable("contrib/libs/x.h"))
        self.assertTrue(is_generated("ydb/core/protos/x.pb.h"))
        self.assertFalse(is_generated("ydb/core/x.h"))


class BuildWorklistTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-worklist-"))

    def test_end_to_end(self) -> None:
        hot = self.tmp / "hot_headers.csv"
        with hot.open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["file", "total_parse_s", "num_tus", "avg_per_tu_ms", "source"])
            w.writerow(["/w/ydb/core/base/blobstorage.h", "577.0", "2244", "257", "x"])
            w.writerow(["/w/ydb/core/base/cheap.h", "5.0", "10", "5", "x"])
            w.writerow(["/w/contrib/libs/protobuf/descriptor.pb.h", "540.0", "4066", "133", "x"])

        graph = self.tmp / "graph.json"
        graph.write_text(json.dumps({
            "nodes": [
                {"path": "ydb/core/base/blobstorage.h", "kind": "header", "fanin": 2244},
                {"path": "contrib/libs/protobuf/descriptor.pb.h", "kind": "header", "fanin": 4066},
            ],
            "edges": [
                {"from": "ydb/core/base/blobstorage.h",
                 "to": "contrib/libs/protobuf/descriptor.pb.h"},
            ],
        }), encoding="utf-8")

        costs = load_costs(hot)
        self.assertIn("ydb/core/base/blobstorage.h", costs)
        self.assertIn("contrib/libs/protobuf/descriptor.pb.h", costs)
        fanin, children = load_graph(graph)

        out = self.tmp / "out"
        build_worklist(costs, fanin, children, out, top=10, min_child_s=20.0)

        md = (out / "worklist.md").read_text(encoding="utf-8")
        # blobstorage.h must be listed and flagged as pulling in the heavy pb.
        self.assertIn("ydb/core/base/blobstorage.h", md)
        self.assertIn("descriptor.pb.h", md)
        self.assertIn("protobuf", md.lower())
        # The edge-to-cut section must rank the pb header.
        self.assertIn("include edges to cut", md.lower())

        # CSV: blobstorage row present, cheap.h ranked lower / present.
        with (out / "worklist.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        headers = [r["header"] for r in rows]
        self.assertEqual(headers[0], "ydb/core/base/blobstorage.h")


if __name__ == "__main__":
    unittest.main()
