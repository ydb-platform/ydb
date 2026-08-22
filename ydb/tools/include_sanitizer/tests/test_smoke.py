"""End-to-end smoke tests using a synthetic mini-project.

These tests do NOT require ``ya make`` or a real ``clang-include-cleaner``
binary. We construct a fake per-file analysis cache directly, then drive
the aggregator and report layers.
"""

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Dict


HERE = Path(__file__).resolve().parent
# Make the package importable when run from a source checkout via
# `python3 -m unittest discover -s ydb/tools/include_sanitizer/tests`
# (run from the repo root). In a ya PY3TEST the package is bundled and
# already importable; this insert is then a harmless no-op.
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.aggregate.graph import (  # noqa: E402
    VERDICT_KEEP_DEFERRED,
    VERDICT_KEEP_PRAGMA,
    VERDICT_MOVE_TO_CPP,
    VERDICT_REMOVE,
    build_graph,
    classify,
)
from ydb.tools.include_sanitizer.analyze.source_includes import scan_includes  # noqa: E402
from ydb.tools.include_sanitizer.report.diff_preview import (  # noqa: E402
    render_header_diff,
    render_tu_addition_diff,
)


class SourceIncludesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = Path(tempfile.mkdtemp(prefix="ydb-iclean-test-"))

    def _write(self, name: str, body: str) -> Path:
        p = self.tmpdir / name
        p.write_text(textwrap.dedent(body), encoding="utf-8")
        return p

    def test_basic_includes(self) -> None:
        p = self._write("a.h", """\
            #pragma once
            #include <vector>
            #include "foo.h"
            #include "bar.h" // IWYU pragma: keep
            #include "baz.h" /* IWYU pragma: export */
            // #include "ignored.h"
        """)
        results = scan_includes(p)
        spelled = [r.spelled for r in results]
        self.assertEqual(spelled, ["vector", "foo.h", "bar.h", "baz.h"])
        self.assertTrue(results[0].angled)
        self.assertTrue(results[2].iwyu_keep)
        self.assertTrue(results[3].iwyu_export)
        self.assertFalse(any(r.iwyu_keep for r in results[:2]))

    def test_block_comments_and_if_zero(self) -> None:
        p = self._write("b.h", """\
            /* #include "comment.h" */
            #if 0
            #include "disabled.h"
            #endif
            #include "live.h"
        """)
        results = scan_includes(p)
        self.assertEqual([r.spelled for r in results], ["disabled.h", "live.h"])
        self.assertTrue(results[0].in_conditional)
        self.assertFalse(results[1].in_conditional)


class ClassifyTest(unittest.TestCase):
    """Drive the verdict logic without invoking clang.

    Build a tiny graph:
        tu.cpp -> H1.h -> H2.h
        tu.cpp uses Sym from H2.h
        H1.h does not use Sym from H2.h

    Expected: include "H2.h" in H1.h is move-to-cpp.
    """

    def _make_per_file(self, files: Dict[str, dict]) -> Dict[str, dict]:
        return files

    def test_remove(self) -> None:
        files: Dict[str, dict] = {
            "H1.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "Dead.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": ["Dead.h"],
                "suggested_inserts": [],
                "include_tree": {"H1.h": ["Dead.h"], "Dead.h": []},
            },
            "tu.cpp": {
                "kind": "tu",
                "includes": [
                    {"line": 1, "spelled": "H1.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                "suggested_inserts": [],
                "include_tree": {"tu.cpp": ["H1.h"], "H1.h": ["Dead.h"], "Dead.h": []},
            },
        }
        graph = build_graph(self._make_per_file(files))
        verdicts = classify(files, graph)
        v = next(v for v in verdicts if v.in_file == "H1.h" and v.spelled == "Dead.h")
        self.assertEqual(v.verdict, VERDICT_REMOVE)

    def test_move_to_cpp(self) -> None:
        files: Dict[str, dict] = {
            "H1.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "H2.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": ["H2.h"],
                "suggested_inserts": [],
                "include_tree": {"H1.h": ["H2.h"], "H2.h": []},
            },
            "tu.cpp": {
                "kind": "tu",
                "includes": [
                    {"line": 1, "spelled": "H1.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                "suggested_inserts": ["H2.h"],
                "include_tree": {"tu.cpp": ["H1.h"], "H1.h": ["H2.h"], "H2.h": []},
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        v = next(v for v in verdicts if v.in_file == "H1.h" and v.spelled == "H2.h")
        self.assertEqual(v.verdict, VERDICT_MOVE_TO_CPP)
        self.assertIn("tu.cpp", v.consumers_needing)

    def test_cycle_error_classified_distinctly(self) -> None:
        from ydb.tools.include_sanitizer.aggregate.graph import VERDICT_CYCLE, VERDICT_PROBE_FAILED
        files: Dict[str, dict] = {
            "cyc.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "defs.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                "suggested_inserts": [],
                "include_tree": {"cyc.h": ["defs.h"], "defs.h": []},
                "error": "include-cycle (not a missing-include problem): "
                         "cyc.h:7:8: error: redefinition of 'X'",
            },
            "broken.h": {
                "kind": "header",
                "includes": [
                    {"line": 2, "spelled": "x.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                "suggested_inserts": [],
                "include_tree": {"broken.h": ["x.h"], "x.h": []},
                "error": "broken.h:4:10: fatal error: 'missing.h' file not found",
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        cyc = next(v for v in verdicts if v.in_file == "cyc.h")
        broken = next(v for v in verdicts if v.in_file == "broken.h")
        self.assertEqual(cyc.verdict, VERDICT_CYCLE)
        self.assertEqual(broken.verdict, VERDICT_PROBE_FAILED)

    def test_move_to_cpp_resolves_relative_sibling_to_source_root(self) -> None:
        """Regression: a header in synclog/ spells a sibling relatively
        ("ctx.h"); a consuming .cpp lives in a subdirectory. The moved
        include must be the source-root path, not the relative spelling
        (which would not resolve from the subdir)."""
        from ydb.tools.include_sanitizer.aggregate.graph import VERDICT_MOVE_TO_CPP
        files: Dict[str, dict] = {
            "ydb/a/synclog/h.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "ctx.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": ["ctx.h"],
                "suggested_inserts": [],
                "include_tree": {"ydb/a/synclog/h.h": ["ydb/a/synclog/ctx.h"],
                                 "ydb/a/synclog/ctx.h": []},
            },
            "ydb/a/synclog/ctx.h": {
                "kind": "header", "includes": [], "unused_includes": [],
                "suggested_inserts": [], "include_tree": {"ydb/a/synclog/ctx.h": []},
            },
            "ydb/a/synclog/sub/tu.cpp": {
                "kind": "tu",
                "includes": [
                    {"line": 1, "spelled": "ydb/a/synclog/h.h", "angled": True,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                # cleaner spells the needed insert from source root:
                "suggested_inserts": ["ydb/a/synclog/ctx.h"],
                "include_tree": {"ydb/a/synclog/sub/tu.cpp": ["ydb/a/synclog/h.h"],
                                 "ydb/a/synclog/h.h": ["ydb/a/synclog/ctx.h"],
                                 "ydb/a/synclog/ctx.h": []},
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        v = next(x for x in verdicts
                 if x.in_file == "ydb/a/synclog/h.h" and x.spelled == "ctx.h")
        self.assertEqual(v.verdict, VERDICT_MOVE_TO_CPP)
        # The add-target must be the source-root path, not "ctx.h".
        self.assertEqual(v.resolved, "ydb/a/synclog/ctx.h")

    def test_move_to_cpp_with_spelling_mismatch(self) -> None:
        """Header spells the include relative; TU's suggested insert is
        source-root spelled. Path resolution must still match them."""
        files: Dict[str, dict] = {
            "ydb/x/h1.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "h2.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": ["h2.h"],
                "suggested_inserts": [],
                "include_tree": {"ydb/x/h1.h": ["ydb/x/h2.h"], "ydb/x/h2.h": []},
            },
            "ydb/x/h2.h": {
                "kind": "header",
                "includes": [],
                "unused_includes": [],
                "suggested_inserts": [],
                "include_tree": {"ydb/x/h2.h": []},
            },
            "ydb/x/tu.cpp": {
                "kind": "tu",
                "includes": [
                    {"line": 1, "spelled": "ydb/x/h1.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                # cleaner spells the insert from source root, not relative:
                "suggested_inserts": ["ydb/x/h2.h"],
                "include_tree": {"ydb/x/tu.cpp": ["ydb/x/h1.h"],
                                 "ydb/x/h1.h": ["ydb/x/h2.h"], "ydb/x/h2.h": []},
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        v = next(v for v in verdicts if v.in_file == "ydb/x/h1.h" and v.spelled == "h2.h")
        self.assertEqual(v.verdict, VERDICT_MOVE_TO_CPP,
                         msg=f"expected move-to-cpp, got {v.verdict} ({v.reason})")
        self.assertIn("ydb/x/tu.cpp", v.consumers_needing)

    def test_keep_pragma(self) -> None:
        files: Dict[str, dict] = {
            "H1.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "Keeper.h", "angled": False,
                     "has_iwyu_keep": True, "has_iwyu_export": False},
                ],
                "unused_includes": ["Keeper.h"],
                "suggested_inserts": [],
                "include_tree": {"H1.h": ["Keeper.h"], "Keeper.h": []},
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        v = next(v for v in verdicts if v.in_file == "H1.h" and v.spelled == "Keeper.h")
        self.assertEqual(v.verdict, VERDICT_KEEP_PRAGMA)

    def test_keep_deferred(self) -> None:
        """When another header (H0) reaches H2.h via H1.h, we keep H2 in H1 for now."""
        files: Dict[str, dict] = {
            "H0.h": {
                "kind": "header",
                "includes": [
                    {"line": 1, "spelled": "H1.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": [],
                "suggested_inserts": ["H2.h"],
                "include_tree": {"H0.h": ["H1.h"], "H1.h": ["H2.h"], "H2.h": []},
            },
            "H1.h": {
                "kind": "header",
                "includes": [
                    {"line": 3, "spelled": "H2.h", "angled": False,
                     "has_iwyu_keep": False, "has_iwyu_export": False},
                ],
                "unused_includes": ["H2.h"],
                "suggested_inserts": [],
                "include_tree": {"H1.h": ["H2.h"], "H2.h": []},
            },
        }
        graph = build_graph(files)
        verdicts = classify(files, graph)
        v = next(v for v in verdicts if v.in_file == "H1.h" and v.spelled == "H2.h")
        self.assertEqual(v.verdict, VERDICT_KEEP_DEFERRED)
        self.assertIn("H0.h", v.headers_blocking)


class DiffPreviewTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-diff-"))

    def test_header_diff_removes_lines(self) -> None:
        h = self.tmp / "H.h"
        h.write_text(
            "#pragma once\n"
            '#include "keep.h"\n'
            '#include "drop.h"\n'
            "int x;\n",
            encoding="utf-8",
        )
        diff = render_header_diff("H.h", self.tmp, [3])
        self.assertIn('-#include "drop.h"', diff)
        self.assertNotIn('-#include "keep.h"', diff)

    def test_self_contained_file_edit_adds_and_removes(self) -> None:
        from ydb.tools.include_sanitizer.report.selfcontain import render_file_edit
        f = self.tmp / "T.cpp"
        f.write_text(
            '#include "keep.h"\n'
            '#include "dead.h"\n'
            '#include "pinned.h" // IWYU pragma: keep\n'
            "int main() {}\n",
            encoding="utf-8",
        )
        fa = {
            "kind": "tu",
            "includes": [
                {"line": 1, "spelled": "keep.h", "angled": False,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
                {"line": 2, "spelled": "dead.h", "angled": False,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
                {"line": 3, "spelled": "pinned.h", "angled": False,
                 "has_iwyu_keep": True, "has_iwyu_export": False},
            ],
            "unused_includes": ["dead.h", "pinned.h"],
            "suggested_inserts": ["util/system/types.h"],
        }
        # Default is add-only: no removals, just the missing include.
        diff_add, n_rm0, n_add0 = render_file_edit("T.cpp", fa, self.tmp)
        self.assertEqual(n_rm0, 0, "default must not remove anything")
        self.assertEqual(n_add0, 1)
        self.assertIn('+#include <util/system/types.h>', diff_add)
        self.assertNotIn('-#include "dead.h"', diff_add)

        # With removals enabled: dead.h removed, pinned.h kept (IWYU keep).
        diff, n_rm, n_add = render_file_edit(
            "T.cpp", fa, self.tmp, do_removals=True)
        self.assertEqual(n_rm, 1, "pinned.h must not be removed (IWYU keep)")
        self.assertEqual(n_add, 1)
        self.assertIn('-#include "dead.h"', diff)
        self.assertNotIn('-#include "pinned.h"', diff)
        self.assertIn('+#include <util/system/types.h>', diff)

    def test_self_contain_is_idempotent(self) -> None:
        """After applying the edit, regenerating must yield an empty diff
        (positions come from the current file, dedup vs current file)."""
        from ydb.tools.include_sanitizer.report.selfcontain import render_file_edit
        f = self.tmp / "U.cpp"
        f.write_text(
            '#include "keep.h"\n'
            '#include "dead.h"\n'
            "int main() {}\n",
            encoding="utf-8",
        )
        fa = {
            "kind": "tu",
            "includes": [
                {"line": 1, "spelled": "keep.h", "angled": False,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
                {"line": 2, "spelled": "dead.h", "angled": False,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
            ],
            "unused_includes": ["dead.h"],
            "suggested_inserts": ["util/system/types.h"],
        }
        diff1, n_rm1, n_add1 = render_file_edit(
            "U.cpp", fa, self.tmp, do_removals=True)
        self.assertEqual((n_rm1, n_add1), (1, 1))
        # Simulate applying the edit: remove dead.h, add the angled include.
        f.write_text(
            '#include "keep.h"\n'
            '#include <util/system/types.h>\n'
            "int main() {}\n",
            encoding="utf-8",
        )
        # Re-run against the SAME (stale) cache: must be a no-op now.
        diff2, n_rm2, n_add2 = render_file_edit(
            "U.cpp", fa, self.tmp, do_removals=True)
        self.assertEqual((n_rm2, n_add2), (0, 0),
                         msg=f"expected no-op on re-run, got diff:\n{diff2}")
        self.assertEqual(diff2, "")

    def test_self_contain_protobuf_filter(self) -> None:
        """--only-pattern must restrict adds/removes to matching includes."""
        import re as _re
        from ydb.tools.include_sanitizer.report.selfcontain import render_file_edit
        f = self.tmp / "H.h"
        f.write_text(
            '#include <ydb/core/protos/foo.pb.h>\n'
            '#include <util/generic/string.h>\n'
            "struct X {};\n",
            encoding="utf-8",
        )
        fa = {
            "kind": "header",
            "includes": [
                {"line": 1, "spelled": "ydb/core/protos/foo.pb.h", "angled": True,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
                {"line": 2, "spelled": "util/generic/string.h", "angled": True,
                 "has_iwyu_keep": False, "has_iwyu_export": False},
            ],
            # both flagged unused, but the filter must only touch the .pb.h
            "unused_includes": ["ydb/core/protos/foo.pb.h", "util/generic/string.h"],
            "suggested_inserts": [],
        }
        rx = _re.compile(r"\.(grpc\.)?pb\.h$")
        diff, n_rm, n_add = render_file_edit(
            "H.h", fa, self.tmp, do_removals=True,
            include_filter=lambda p: bool(rx.search(p)))
        self.assertEqual(n_rm, 1)
        self.assertIn('-#include <ydb/core/protos/foo.pb.h>', diff)
        self.assertNotIn('-#include <util/generic/string.h>', diff)

    def test_tu_diff_adds_after_last_include(self) -> None:
        tu = self.tmp / "T.cpp"
        tu.write_text(
            '#include "H1.h"\n'
            '#include "H2.h"\n'
            "int main() {}\n",
            encoding="utf-8",
        )
        diff = render_tu_addition_diff("T.cpp", self.tmp, ["ydb/x/new.h"])
        self.assertIn('+#include <ydb/x/new.h>', diff)


class HeaderProbePreambleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="ydb-iclean-probe-pre-"))

    def test_synthesize_probe_with_preamble_writes_preceding_includes(self) -> None:
        from ydb.tools.include_sanitizer.analyze.header_probe import synthesize_probe
        probe = synthesize_probe(
            "ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_utils.h",
            self.tmp / "fake_repo",
            self.tmp / "probes",
            preamble_includes=[
                "ydb/core/blobstorage/vdisk/hulldb/base/hullds_sst_it_all.h",
                "ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h",
            ],
        )
        text = probe.probe_path.read_text(encoding="utf-8")
        self.assertIn('#include "ydb/core/blobstorage/vdisk/hulldb/base/hullds_sst_it_all.h"', text)
        self.assertIn('#include "ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h"', text)
        # The preamble must precede the target #include directive
        # specifically (not the comment header, which also mentions
        # the target path).
        preamble_idx = text.index(
            '#include "ydb/core/blobstorage/vdisk/hulldb/base/hullds_sst_it_all.h"'
        )
        target_idx = text.index(
            '#include "ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_utils.h"'
        )
        self.assertLess(preamble_idx, target_idx,
                        msg=f"preamble must precede target include:\n{text}")

    def test_synthesize_probe_without_preamble_unchanged(self) -> None:
        from ydb.tools.include_sanitizer.analyze.header_probe import synthesize_probe
        probe = synthesize_probe(
            "ydb/example.h",
            self.tmp / "fake_repo",
            self.tmp / "probes",
        )
        text = probe.probe_path.read_text(encoding="utf-8")
        self.assertEqual(
            text,
            "// Auto-generated probe TU for ydb/example.h\n"
            '#include "ydb/example.h"\n',
        )

    def test_tu_preamble_returns_includes_before_target(self) -> None:
        from ydb.tools.include_sanitizer.analyze.per_tu import _tu_preamble_for_header
        repo = self.tmp / "repo"
        (repo / "ydb/core").mkdir(parents=True)
        tu = repo / "ydb/core/x.cpp"
        tu.write_text(
            "#include \"ydb/core/base/defs.h\"\n"
            "#include \"ydb/core/base/blobstorage.h\"\n"
            "#include \"ydb/core/x.h\"\n"
            "// rest of TU\n",
            encoding="utf-8",
        )
        preamble = _tu_preamble_for_header(
            str(tu), "ydb/core/x.h", repo,
        )
        self.assertEqual(
            preamble,
            ["ydb/core/base/defs.h", "ydb/core/base/blobstorage.h"],
        )

    def test_tu_preamble_missing_tu_returns_none(self) -> None:
        from ydb.tools.include_sanitizer.analyze.per_tu import _tu_preamble_for_header
        self.assertIsNone(_tu_preamble_for_header(
            "/nonexistent/path.cpp", "ydb/x.h", self.tmp,
        ))
        self.assertIsNone(_tu_preamble_for_header("", "ydb/x.h", self.tmp))


if __name__ == "__main__":
    unittest.main()
