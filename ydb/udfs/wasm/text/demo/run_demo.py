#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Text WASM demo queries built in-process (no checked-in .sql files).

Shapes (bridge Text::* via BridgeEnsureString):
  probes  — K distinct Module::byte_at (O(1); pin once, reuse K times)
  letters — Module::count_letters (O(n) body, one call)
  multi   — letters + digits + upper (three O(n) exports)
  length  — Module::text_length (O(1) body, one call)

Examples:
  python3 run_demo.py --readable
  python3 run_demo.py --evidence
  python3 run_demo.py --tables text_1mb --shapes probes letters --runs 5
  python3 run_demo.py --native --tables text_1mb --shapes probes length
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import subprocess
import sys
import time
from collections import Counter

TABLES = ("text_200kb", "text_1mb")
SHAPES = ("probes", "letters", "multi", "length")
PROBE_COUNT = 16


def probes_projection(module: str, k: int) -> str:
    terms = ["%s::byte_at(txt, %s)" % (module, i) for i in range(k)]
    return "SUM(" + " + ".join(terms) + ")"


def shape_projection(module: str, shape: str, probe_count: int) -> str:
    if shape == "probes":
        return probes_projection(module, probe_count)
    if shape == "letters":
        return "SUM(%s::count_letters(txt))" % module
    if shape == "multi":
        return (
            "SUM(%s::count_letters(txt)"
            " + %s::count_digits(txt)"
            " + %s::count_upper(txt))" % (module, module, module)
        )
    if shape == "length":
        return "SUM(%s::text_length(txt))" % module
    raise SystemExit("unknown shape: %s" % shape)


def sql_load(table: str, module: str, shape: str, probe_count: int) -> str:
    projection = shape_projection(module, shape, probe_count)
    return (
        "/* syntax version 1 */\n"
        "SELECT {projection} AS checksum\n"
        "FROM `{table}`;\n"
    ).format(projection=projection, table=table)


def sql_readable(table: str, module: str, limit: int) -> str:
    return (
        "/* syntax version 1 */\n"
        "SELECT\n"
        "    id,\n"
        "    SUBSTRING(txt, 0u, 48u) AS head,\n"
        "    {module}::count_letters(txt) AS letters,\n"
        "    {module}::count_digits(txt) AS digits,\n"
        "    {module}::count_upper(txt) AS upper,\n"
        "    {module}::text_length(txt) AS len,\n"
        "    {module}::byte_at(txt, 0) AS b0\n"
        "FROM `{table}`\n"
        "WHERE id <= {limit}ul\n"
        "ORDER BY id;\n"
    ).format(module=module, table=table, limit=limit)


def sql_evidence(table: str, module: str) -> str:
    return (
        "/* syntax version 1 */\n"
        "SELECT id,\n"
        "       {module}::count_letters(txt) AS letters,\n"
        "       {module}::byte_at(txt, 0) AS b0,\n"
        "       {module}::text_length(txt) AS len\n"
        "FROM `{table}`\n"
        "WHERE id = 1ul;\n"
    ).format(module=module, table=table)


def run_sql(ydb: str, endpoint: str, database: str, sql: str, stats: str | None) -> str:
    cmd = [ydb, "-e", endpoint, "-d", database, "sql", "-s", sql]
    if stats:
        cmd.extend(["--stats", stats])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0 or re.search(r"^(Status:|Issues:)", out, re.M):
        raise RuntimeError("query failed:\n%s" % out)
    return out


def parse_timed(out: str, wall_ms: int) -> tuple[int, int, str]:
    m = re.search(r"total_cpu_time_us:\s*(\d+)", out) or re.search(r"cpu_time_us:\s*(\d+)", out)
    cpu = int(m.group(1)) if m else 0
    cache = "cache" if "from_cache: true" in out else "cold"
    return wall_ms, cpu, cache


def median_int(values: list[int]) -> int:
    if not values:
        return 0
    return int(statistics.median(values))


def timed_once(
    ydb: str, endpoint: str, database: str, sql: str
) -> tuple[int, int, str]:
    t0 = time.perf_counter_ns()
    out = run_sql(ydb, endpoint, database, sql, stats="full")
    wall_ms = int((time.perf_counter_ns() - t0) / 1_000_000)
    return parse_timed(out, wall_ms)


def parse_list(raw: str, allowed: tuple[str, ...], what: str) -> list[str]:
    items = [x.strip() for x in raw.replace(",", " ").split() if x.strip()]
    if not items:
        raise SystemExit("empty %s" % what)
    bad = [x for x in items if x not in allowed]
    if bad:
        raise SystemExit("unknown %s: %s (allowed: %s)" % (what, bad, ", ".join(allowed)))
    return items


def cmd_readable(args: argparse.Namespace) -> int:
    sql = sql_readable(args.readable_table, args.module, args.readable_rows)
    print(run_sql(args.ydb, args.endpoint, args.database, sql, stats=None), end="")
    return 0


def cmd_evidence(args: argparse.Namespace) -> int:
    sql = sql_evidence(args.readable_table, args.module)
    print("evidence: 1-row count_letters + text_length (bridge EnsureString)")
    print(run_sql(args.ydb, args.endpoint, args.database, sql, stats=None), end="")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    tables = parse_list(args.tables, TABLES, "tables")
    shapes = parse_list(args.shapes, SHAPES, "shapes")
    if args.warmup < 1:
        raise SystemExit("warmup must be >= 1")

    modules = [("wasm", "Text")]
    if args.native:
        modules.append(("native", "TextNative"))

    print(
        "measuring tables=%s shapes=%s runs=%s after %s warmup (excluded)"
        % (tables, shapes, args.runs, args.warmup)
    )
    print("  endpoint=%s db=%s" % (args.endpoint, args.database))
    if args.native:
        print("  modules=Text + TextNative")
    print()

    if args.native:
        print(
            "  %-12s %-8s %-12s %-14s %-12s %-14s %s"
            % ("table", "shape", "wasm_ms", "wasm_cpu_us", "nat_ms", "nat_cpu_us", "cache")
        )
    else:
        print(
            "  %-12s %-8s %-12s %-14s %s"
            % ("table", "shape", "wasm_ms", "wasm_cpu_us", "cache")
        )

    summary: list[str] = []
    for table in tables:
        for shape in shapes:
            samples: dict[str, list[tuple[int, int, str]]] = {tag: [] for tag, _ in modules}
            for tag, module in modules:
                sql = sql_load(table, module, shape, args.probes)
                for _ in range(args.warmup):
                    timed_once(args.ydb, args.endpoint, args.database, sql)
                for _ in range(args.runs):
                    samples[tag].append(
                        timed_once(args.ydb, args.endpoint, args.database, sql)
                    )

            wasm_ms = median_int([s[0] for s in samples["wasm"]])
            wasm_cpu = median_int([s[1] for s in samples["wasm"]])
            w_cache = ",".join(
                "%s %s" % (n, k) for k, n in Counter(s[2] for s in samples["wasm"]).items()
            )
            cold = any(s[2] == "cold" for s in samples["wasm"])

            if args.native:
                nat_ms = median_int([s[0] for s in samples["native"]])
                nat_cpu = median_int([s[1] for s in samples["native"]])
                n_cache = ",".join(
                    "%s %s" % (n, k)
                    for k, n in Counter(s[2] for s in samples["native"]).items()
                )
                cold = cold or any(s[2] == "cold" for s in samples["native"])
                print(
                    "  %-12s %-8s %-12s %-14s %-12s %-14s %s/%s"
                    % (table, shape, wasm_ms, wasm_cpu, nat_ms, nat_cpu, w_cache, n_cache)
                )
                summary.append(
                    "%s %s %s %s %s %s" % (table, shape, wasm_ms, wasm_cpu, nat_ms, nat_cpu)
                )
            else:
                print(
                    "  %-12s %-8s %-12s %-14s %s"
                    % (table, shape, wasm_ms, wasm_cpu, w_cache)
                )
                summary.append("%s %s %s %s" % (table, shape, wasm_ms, wasm_cpu))

            if cold:
                print(
                    "    warning: a measured run compiled from scratch (from_cache != true)",
                    file=sys.stderr,
                )

    print()
    print("summary (medians of %s runs, warmup=%s excluded):" % (args.runs, args.warmup))
    for line in summary:
        print(line)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Text UDF demo: build SQL and run")
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", "grpc://localhost:2146"))
    parser.add_argument("--database", default=os.environ.get("DB", "/Root/test"))
    parser.add_argument("--ydb", default=os.environ.get("YDB", "ydb"))
    parser.add_argument("--module", default="Text", help="YQL module for --readable/--evidence")
    parser.add_argument("--readable", action="store_true")
    parser.add_argument("--evidence", action="store_true")
    parser.add_argument("--readable-table", default="text_200kb")
    parser.add_argument("--readable-rows", type=int, default=5)
    parser.add_argument("--tables", default=os.environ.get("TABLES", "text_1mb"))
    parser.add_argument("--shapes", default=os.environ.get("SHAPES", "probes letters"))
    parser.add_argument("--probes", type=int, default=PROBE_COUNT)
    parser.add_argument("--runs", type=int, default=int(os.environ.get("RUNS", "5")))
    parser.add_argument("--warmup", type=int, default=int(os.environ.get("WARMUP", "1")))
    parser.add_argument(
        "--native",
        action="store_true",
        default=os.environ.get("NATIVE", "0") == "1",
        help="also time TextNative",
    )
    args = parser.parse_args()

    if args.readable:
        return cmd_readable(args)
    if args.evidence:
        return cmd_evidence(args)
    return cmd_bench(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
