#!/usr/bin/env python3
"""Unit tests for evlog / compile-profile → ci_metrics export."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from export_ya_nodes import (
    main,
    nodes_from_cpp_json,
    nodes_from_evlog,
    nodes_from_headers_json,
    parse_node_name,
    pick_latest_file,
    resolve_input_files,
)


class ParseNodeNameTest(unittest.TestCase):
    def test_compile_with_build_root(self):
        kind, path = parse_node_name("Compile($B/ydb/core/tablet/tablet.cpp)")
        self.assertEqual(kind, "Compile")
        self.assertEqual(path, "ydb/core/tablet/tablet.cpp")

    def test_link(self):
        kind, path = parse_node_name("Link($(BUILD_ROOT)/ydb/apps/ydbd/ydbd)")
        self.assertEqual(kind, "Link")
        self.assertEqual(path, "ydb/apps/ydbd/ydbd")

    def test_shared_library(self):
        kind, path = parse_node_name("SharedLibrary($B/ydb/library/yql/libyql.so)")
        self.assertEqual(kind, "SharedLibrary")
        self.assertEqual(path, "ydb/library/yql/libyql.so")


class EvlogNodesTest(unittest.TestCase):
    def test_node_finished_skips_tests(self):
        events = [
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {"name": "Compile($B/ydb/foo.cpp)", "time": [10.0, 12.5]},
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {"name": "Run(rnd-abc$(BUILD_ROOT)/ydb/tests/foo/test-results/chunk0/)", "time": [1, 20]},
            },
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {"name": "Link($B/ydb/apps/ydbd/ydbd)", "time": [20.0, 21.0]},
            },
        ]
        nodes = nodes_from_evlog(events)
        names = {node["name"]: node for node in nodes}
        self.assertIn("ydb/foo.cpp", names)
        self.assertEqual(names["ydb/foo.cpp"]["duration_ms"], 2500.0)
        self.assertEqual(names["ydb/foo.cpp"]["node_kind"], "Compile")
        self.assertIn("ydb/apps/ydbd/ydbd", names)
        self.assertEqual(names["ydb/apps/ydbd/ydbd"]["node_kind"], "Link")
        self.assertTrue(all(not name.startswith("Run") for name in names))

    def test_chrome_be_pair(self):
        events = [
            {"ph": "B", "pid": 1, "tid": 2, "ts": 1_000_000, "name": "Compile", "args": {"name": "Compile($B/ydb/a.cpp)"}},
            {"ph": "E", "pid": 1, "tid": 2, "ts": 3_000_000, "name": "Compile"},
        ]
        nodes = nodes_from_evlog(events)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["name"], "ydb/a.cpp")
        self.assertEqual(nodes[0]["duration_ms"], 2000.0)

    def test_prefers_node_finished_over_chrome(self):
        events = [
            {
                "namespace": "worker_threads",
                "event": "node-finished",
                "value": {"name": "Compile($B/ydb/a.cpp)", "time": [1.0, 2.0]},
            },
            {"ph": "B", "pid": 1, "tid": 2, "ts": 1_000_000, "name": "Compile", "args": {"name": "Compile($B/ydb/a.cpp)"}},
            {"ph": "E", "pid": 1, "tid": 2, "ts": 3_000_000, "name": "Compile"},
        ]
        nodes = nodes_from_evlog(events)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["duration_ms"], 1000.0)


class CppJsonTest(unittest.TestCase):
    def test_keeps_every_file_time(self):
        nodes = nodes_from_cpp_json(
            {
                "total_compilation_time": 12.5,
                "cpp_compilation_times": [
                    {"path": "src/ydb/a.cpp", "time_s": 4.0},
                    {"path": "src/ydb/b.cpp", "time_s": 0.1},
                    {"path": "", "time_s": 9},
                ],
            }
        )
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0]["duration_ms"], 4000.0)
        self.assertEqual(nodes[1]["duration_ms"], 100.0)


class HeadersJsonTest(unittest.TestCase):
    def test_writes_raw_mean_and_count(self):
        nodes = nodes_from_headers_json(
            {
                "headers_compile_duration": [
                    {"path": "src/ydb/defs.h", "mean_compilation_time_s": 0.5, "inclusion_count": 4},
                    {"path": "", "mean_compilation_time_s": 9, "inclusion_count": 1},
                ]
            }
        )
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0]["name"], "src/ydb/defs.h")
        self.assertEqual(nodes[0]["node_kind"], "Header")
        self.assertEqual(nodes[0]["duration_ms"], 500.0)
        self.assertEqual(nodes[0]["inclusion_count"], 4)


class ResolveFilesTest(unittest.TestCase):
    def test_glob_picks_last_try(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = os.path.join(tmp, "try_1")
            second = os.path.join(tmp, "try_2")
            os.makedirs(first)
            os.makedirs(second)
            path1 = os.path.join(first, "ya_evlog.jsonl")
            path2 = os.path.join(second, "ya_evlog.jsonl")
            for path in (path1, path2):
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("{}\n")
            matches = resolve_input_files(os.path.join(tmp, "try_*/ya_evlog.jsonl"))
            self.assertEqual(matches, [path1, path2])
            self.assertEqual(pick_latest_file(matches), path2)

    def test_glob_picks_try_10_over_try_2(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for n in (2, 10, 9):
                try_dir = os.path.join(tmp, f"try_{n}")
                os.makedirs(try_dir)
                path = os.path.join(try_dir, "ya_evlog.jsonl")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write("{}\n")
                paths.append(path)
            matches = resolve_input_files(os.path.join(tmp, "try_*/ya_evlog.jsonl"))
            self.assertEqual(pick_latest_file(matches), os.path.join(tmp, "try_10", "ya_evlog.jsonl"))
            self.assertNotEqual(pick_latest_file(matches), os.path.join(tmp, "try_9", "ya_evlog.jsonl"))

    def test_missing_glob(self):
        self.assertEqual(resolve_input_files("/no/such/try_*/ya_evlog.jsonl"), [])
        self.assertIsNone(pick_latest_file([]))


class WriteSmokeTest(unittest.TestCase):
    def test_cli_writes_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            evlog = f"{tmp}/evlog.jsonl"
            out = f"{tmp}/metrics.jsonl"
            with open(evlog, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "namespace": "worker_threads",
                            "event": "node-finished",
                            "value": {"name": "Compile($B/ydb/x.cpp)", "time": [1.0, 2.0]},
                        }
                    )
                    + "\n"
                )
            self.assertEqual(main(["--evlog", evlog, "--file", out, "--source", "nightly_build"]), 0)
            with open(out, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            names = {row["name"]: row for row in rows}
            self.assertEqual(names["ydb/x.cpp"]["source"], "nightly_build")
            self.assertEqual(names["ydb/x.cpp"]["value"], 1000.0)
            self.assertEqual(names["ydb/x.cpp"]["labels"]["node_kind"], "Compile")
            self.assertEqual(names["build_info"]["kind"], "info")
            self.assertEqual(names["build_info"]["labels"]["payload"]["schema"], "ya_nodes")
            self.assertEqual(names["build_info"]["labels"]["payload"]["nodes"][0]["name"], "ydb/x.cpp")

    def test_cpp_and_headers_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            cpp = f"{tmp}/cpp.json"
            headers = f"{tmp}/headers.json"
            out = f"{tmp}/metrics.jsonl"
            with open(cpp, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "total_compilation_time": 3.5,
                        "cpp_compilation_times": [{"path": "src/ydb/a.cpp", "time_s": 2.0}],
                    },
                    handle,
                )
            with open(headers, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "headers_compile_duration": [
                            {"path": "src/ydb/a.h", "mean_compilation_time_s": 1.0, "inclusion_count": 2}
                        ]
                    },
                    handle,
                )
            self.assertEqual(
                main(
                    [
                        "--cpp-json",
                        cpp,
                        "--headers-json",
                        headers,
                        "--file",
                        out,
                        "--source",
                        "build_bloat",
                    ]
                ),
                0,
            )
            with open(out, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            names = {row["name"]: row for row in rows}
            self.assertNotIn("compile_total", names)
            self.assertEqual(names["src/ydb/a.cpp"]["value"], 2000.0)
            self.assertEqual(names["src/ydb/a.h"]["value"], 1000.0)
            self.assertEqual(names["src/ydb/a.h"]["labels"]["node_kind"], "Header")
            self.assertEqual(names["src/ydb/a.h"]["labels"]["inclusion_count"], 2)
            info_rows = [row for row in rows if row["name"] == "build_info"]
            self.assertEqual(len(info_rows), 2)
            origins = {row["labels"]["payload"]["origin"] for row in info_rows}
            self.assertEqual(origins, {cpp, headers})

    def test_no_build_info_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            evlog = f"{tmp}/evlog.jsonl"
            out = f"{tmp}/metrics.jsonl"
            with open(evlog, "w", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "namespace": "worker_threads",
                            "event": "node-finished",
                            "value": {"name": "Compile($B/ydb/x.cpp)", "time": [1.0, 2.0]},
                        }
                    )
                    + "\n"
                )
            self.assertEqual(
                main(["--evlog", evlog, "--file", out, "--source", "nightly_build", "--no-build-info"]),
                0,
            )
            with open(out, encoding="utf-8") as handle:
                rows = [json.loads(line) for line in handle if line.strip()]
            self.assertEqual([row["name"] for row in rows], ["ydb/x.cpp"])


if __name__ == "__main__":
    unittest.main()
