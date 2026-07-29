"""Pilot regression test against real ``ydb/`` files.

These exercises:

- ``IWYU pragma: keep`` recognition on the one known site in the repo
  (``ydb/core/kqp/runtime/scheduler/tree/snapshot.cpp:3``).
- The conditional ``#if defined(GOOGLE_PROTOBUF_INCLUDED_...)`` block in
  ``ydb/core/base/appdata.h``: the includes that *follow* the block must
  be picked up; the macro definitions inside the block must not be
  counted as includes.
- A small end-to-end run of the pilot harness on a handful of files.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.common import REPO_ROOT  # noqa: E402
from ydb.tools.include_sanitizer.analyze.source_includes import scan_includes  # noqa: E402
from ydb.tools.include_sanitizer.aggregate.graph import (  # noqa: E402
    VERDICT_KEEP_PRAGMA,
    build_graph,
    classify,
)
from ydb.tools.include_sanitizer.pilot import (  # noqa: E402
    iter_files,
    synthesize_file_analyses,
    assert_iwyu_pragmas_respected,
)


SNAPSHOT_CPP = "ydb/core/kqp/runtime/scheduler/tree/snapshot.cpp"
APPDATA_H = "ydb/core/base/appdata.h"

# These tests read real ydb/core source files; they only make sense from
# a full checkout, not a hermetic CI test sandbox. Skip when absent.
_HAS_YDB_CORE = (REPO_ROOT / "ydb" / "core" / "base").exists()


@unittest.skipUnless(_HAS_YDB_CORE, "needs ydb/core source files")
class RealFilesScanTest(unittest.TestCase):
    def test_iwyu_keep_detected(self) -> None:
        results = scan_includes(REPO_ROOT / SNAPSHOT_CPP)
        keeps = [r for r in results if r.iwyu_keep]
        self.assertEqual(len(keeps), 1, f"expected 1 IWYU keep in {SNAPSHOT_CPP}")
        self.assertEqual(keeps[0].spelled, "dynamic.h")

    def test_appdata_conditional_block_does_not_emit_fake_includes(self) -> None:
        results = scan_includes(REPO_ROOT / APPDATA_H)
        spelled = [r.spelled for r in results]
        self.assertNotIn("GOOGLE_PROTOBUF_INCLUDED_ydb_2fcore_2fprotos_2fauth_2eproto", spelled)
        self.assertIn("defs.h", spelled)
        self.assertIn("appdata_fwd.h", spelled)
        self.assertIn("ydb/library/actors/core/actor.h", spelled)

    def test_appdata_has_angled_and_quoted(self) -> None:
        results = scan_includes(REPO_ROOT / APPDATA_H)
        by_spelled = {r.spelled: r for r in results}
        self.assertFalse(by_spelled["defs.h"].angled)
        self.assertTrue(by_spelled["ydb/library/actors/core/actor.h"].angled)


@unittest.skipUnless(_HAS_YDB_CORE, "needs ydb/core source files")
class PilotPipelineTest(unittest.TestCase):
    def test_pilot_respects_iwyu_pragma(self) -> None:
        """An ``IWYU pragma: keep`` must not be classified for removal even
        under the ``random`` policy, which would otherwise be willing to
        flag any header include."""
        files = [
            REPO_ROOT / SNAPSHOT_CPP,
            REPO_ROOT / "ydb/core/kqp/runtime/scheduler/tree/dynamic.h",
        ]
        files = [p for p in files if p.exists()]
        per_file_as = synthesize_file_analyses(files, policy="random", seed=1, repo_root=REPO_ROOT)
        assert_iwyu_pragmas_respected(per_file_as)

        per_file = {rel: an.to_json() for rel, an in per_file_as.items()}
        graph = build_graph(per_file)
        verdicts = classify(per_file, graph)

        kept = [v for v in verdicts if v.in_file == SNAPSHOT_CPP and v.spelled == "dynamic.h"]
        if kept:
            self.assertEqual(kept[0].verdict, VERDICT_KEEP_PRAGMA)

    def test_pilot_runs_on_ydb_core_base(self) -> None:
        files = iter_files(REPO_ROOT, "ydb/core/base")
        self.assertGreater(len(files), 50)
        per_file_as = synthesize_file_analyses(files, policy="random", seed=7, repo_root=REPO_ROOT)
        assert_iwyu_pragmas_respected(per_file_as)

        per_file = {rel: an.to_json() for rel, an in per_file_as.items()}
        graph = build_graph(per_file)
        verdicts = classify(per_file, graph)

        self.assertGreater(len(graph.nodes), 50)
        self.assertGreater(len(verdicts), 100)

        verdict_set = {v.verdict for v in verdicts}
        self.assertIn("keep", verdict_set)
        self.assertIn("remove", verdict_set)


if __name__ == "__main__":
    unittest.main()
