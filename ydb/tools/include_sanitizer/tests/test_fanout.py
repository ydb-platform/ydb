"""Tests for rebuild-fanout ranking and protobuf include classification."""

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
_REPO = HERE.parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from ydb.tools.include_sanitizer.analysis.fanout import (  # noqa: E402
    ProtoInfo,
    classify_proto_use,
    parse_proto,
    proto_importers,
    rank_headers,
    rebuild_set,
    resolve_include,
    scan_graph,
    transitive_proto_importers,
)


class ResolveTest(unittest.TestCase):
    def test_source_root(self) -> None:
        self.assertEqual(
            resolve_include("ydb/core/protos/config.pb.h", "ydb/core/base/x.h"),
            "ydb/core/protos/config.pb.h",
        )

    def test_relative(self) -> None:
        self.assertEqual(
            resolve_include("defs.h", "ydb/core/base/appdata.h"),
            "ydb/core/base/defs.h",
        )

    def test_skips_stdlib(self) -> None:
        self.assertIsNone(resolve_include("vector", "ydb/core/base/x.h"))


class ClassifyTest(unittest.TestCase):
    info = ProtoInfo(
        path="ydb/core/protos/config.proto",
        package="NKikimrConfig",
        messages=["TAppConfig", "TStateStorageConfig", "TFeatureFlags"],
        enums=["EFoo"],
    )

    def _cls(self, body: str) -> str:
        text = '#include <ydb/core/protos/config.pb.h>\n' + textwrap.dedent(body)
        return classify_proto_use(text, "ydb/core/protos/config.pb.h", self.info)

    def test_unused(self) -> None:
        self.assertEqual(self._cls("void f();\n"), "unused")

    def test_const_ref_is_fwd(self) -> None:
        self.assertEqual(
            self._cls("void f(const NKikimrConfig::TStateStorageConfig& c);\n"),
            "fwd",
        )

    def test_ref_no_space_is_fwd(self) -> None:
        self.assertEqual(self._cls("void f(TAppConfig& c);\n"), "fwd")

    def test_pointer_is_fwd(self) -> None:
        self.assertEqual(self._cls("TAppConfig* p;\n"), "fwd")

    def test_tholder_is_fwd(self) -> None:
        self.assertEqual(self._cls("THolder<TAppConfig> p;\n"), "fwd")

    def test_unique_ptr_is_fwd(self) -> None:
        self.assertEqual(self._cls("std::unique_ptr<TFeatureFlags> p;\n"), "fwd")

    def test_value_member_is_keep(self) -> None:
        self.assertEqual(self._cls("TAppConfig Config;\n"), "keep")

    def test_nested_access_is_keep(self) -> None:
        self.assertEqual(self._cls("auto x = TAppConfig::default_instance();\n"),
                         "keep")

    def test_enum_is_keep(self) -> None:
        self.assertEqual(self._cls("EFoo x;\n"), "keep")

    def test_existing_fwd_decl_is_fwd(self) -> None:
        self.assertEqual(self._cls("class TAppConfig;\nvoid f(TAppConfig*);\n"),
                         "fwd")


class ProtoParseTest(unittest.TestCase):
    def test_parse(self) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="ydb-fanout-"))
        p = tmp / "config.proto"
        p.write_text(textwrap.dedent("""\
            import "ydb/core/protos/blobstorage.proto";
            package NKikimrConfig;
            message TAppConfig {
                message Nested { optional int32 x = 1; }
                optional int32 y = 2;
            }
            enum EKind { A = 0; }
        """), encoding="utf-8")
        info = parse_proto(p, "ydb/core/protos/config.proto")
        self.assertEqual(info.package, "NKikimrConfig")
        self.assertEqual(info.messages, ["TAppConfig"])
        self.assertEqual(info.enums, ["EKind"])
        self.assertEqual(info.imports, ["ydb/core/protos/blobstorage.proto"])
        self.assertEqual(info.pb_h, "ydb/core/protos/config.pb.h")


class GraphTest(unittest.TestCase):
    def test_rebuild_and_proto_overlay(self) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="ydb-fanout-g-"))
        (tmp / "ydb" / "core" / "base").mkdir(parents=True)
        (tmp / "ydb" / "core" / "protos").mkdir(parents=True)
        (tmp / "ydb/core/protos/blob.proto").write_text(
            "package N; message B {}\n", encoding="utf-8")
        (tmp / "ydb/core/protos/config.proto").write_text(
            'import "ydb/core/protos/blob.proto";\n'
            "package NKikimrConfig;\nmessage TAppConfig {}\n",
            encoding="utf-8")
        (tmp / "ydb/core/base/amp.h").write_text(
            '#pragma once\n#include <ydb/core/protos/config.pb.h>\n'
            "void f(const NKikimrConfig::TAppConfig&);\n",
            encoding="utf-8")
        (tmp / "ydb/core/base/a.cpp").write_text(
            '#include "amp.h"\n', encoding="utf-8")
        (tmp / "ydb/core/base/direct.cpp").write_text(
            '#include <ydb/core/protos/blob.pb.h>\n', encoding="utf-8")

        # Point PATHS.repo_root-equivalent by scanning tmp as repo.
        graph = scan_graph(tmp, ["ydb"])
        self.assertIn("ydb/core/base/amp.h", graph.files)
        self.assertIn("ydb/core/protos/config.pb.h",
                      graph.out_edges["ydb/core/base/amp.h"])
        self.assertEqual(
            graph.tu_consumers("ydb/core/protos/config.pb.h"),
            {"ydb/core/base/a.cpp"},
        )

        from ydb.tools.include_sanitizer.analysis.fanout import load_protos
        protos = load_protos(tmp, ["ydb"])
        incoming = proto_importers(protos)
        self.assertEqual(
            transitive_proto_importers("ydb/core/protos/blob.proto", incoming),
            {"ydb/core/protos/config.proto"},
        )
        rebuilt = rebuild_set(
            "ydb/core/protos/blob.pb.h", graph, protos, incoming
        )
        # Changing blob.proto regenerates config.pb.h too, so amp.h's TU
        # rebuilds as well as the file that includes blob.pb.h directly.
        self.assertEqual(rebuilt, {"ydb/core/base/a.cpp",
                                   "ydb/core/base/direct.cpp"})

        rows = rank_headers(graph, protos, incoming, only_pb=True)
        by = {r.header: r for r in rows}
        self.assertGreaterEqual(by["ydb/core/protos/blob.pb.h"].rebuild_tus, 2)
        self.assertEqual(by["ydb/core/protos/config.pb.h"].direct_h, 1)
        self.assertFalse(by["ydb/core/protos/config.pb.h"].via_appdata)

        (tmp / "ydb/core/base/appdata.h").write_text(
            '#pragma once\n#include "amp.h"\n', encoding="utf-8")
        (tmp / "ydb/core/base/b.cpp").write_text(
            '#include "appdata.h"\n', encoding="utf-8")
        graph2 = scan_graph(tmp, ["ydb"])
        rows2 = rank_headers(graph2, protos, incoming, only_pb=True)
        by2 = {r.header: r for r in rows2}
        self.assertTrue(by2["ydb/core/protos/config.pb.h"].via_appdata)
        self.assertEqual(
            graph2.tu_consumers("ydb/core/protos/config.pb.h"),
            {"ydb/core/base/a.cpp", "ydb/core/base/b.cpp"},
        )


if __name__ == "__main__":
    unittest.main()
