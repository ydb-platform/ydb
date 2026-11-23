#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run Trie WASM demo queries built in-process (no checked-in .sql files).

Each load query materializes one dictionary ($dict scalar subquery) and scans
every address. Bridge Lookup pins $dict via RegisterOrReuse + BridgeEnsureString.

Examples:
  python3 run_demo.py --readable
  python3 run_demo.py --evidence
  python3 run_demo.py --dict-from 1 --dict-to 3
  python3 run_demo.py --native --dict-from 1 --dict-to 1
"""

from __future__ import annotations

import argparse
import os
import re
import statistics
import subprocess
import sys
import time


def sql_readable(addr_table: str, dict_table: str, module: str, limit: int) -> str:
    return (
        "/* syntax version 1 */\n"
        "$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = 1ul;\n"
        "\n"
        "SELECT\n"
        "    id,\n"
        "    ip,\n"
        "    {module}::LookupWithString(addr, $dict) AS org\n"
        "FROM `{addr}`\n"
        "WHERE id <= {limit}ul\n"
        "ORDER BY id;\n"
    ).format(dict=dict_table, module=module, addr=addr_table, limit=limit)


def sql_evidence(addr_table: str, dict_table: str, module: str) -> str:
    return (
        "/* syntax version 1 */\n"
        "$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = 1ul;\n"
        "\n"
        "SELECT id, ip,\n"
        "       {module}::LookupWithString(addr, $dict) AS org\n"
        "FROM `{addr}`\n"
        "WHERE id = 1ul;\n"
    ).format(dict=dict_table, module=module, addr=addr_table)


def sql_load(addr_table: str, dict_table: str, module: str, dict_id: int) -> str:
    return (
        "/* syntax version 1 */\n"
        "$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = {dict_id}ul;\n"
        "\n"
        "SELECT SUM({module}::Lookup(addr, $dict)) AS checksum\n"
        "FROM `{addr}`;\n"
    ).format(dict=dict_table, dict_id=dict_id, module=module, addr=addr_table)


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


def timed_once(
    ydb: str, endpoint: str, database: str, sql: str
) -> tuple[int, int, str]:
    t0 = time.perf_counter_ns()
    out = run_sql(ydb, endpoint, database, sql, stats="full")
    wall_ms = int((time.perf_counter_ns() - t0) / 1_000_000)
    return parse_timed(out, wall_ms)


def median_int(values: list[int]) -> int:
    if not values:
        return 0
    return int(statistics.median(values))


def cmd_readable(args: argparse.Namespace) -> int:
    sql = sql_readable(args.addr_table, args.dict_table, args.module, args.readable_rows)
    print(run_sql(args.ydb, args.endpoint, args.database, sql, stats=None), end="")
    return 0


def cmd_evidence(args: argparse.Namespace) -> int:
    sql = sql_evidence(args.addr_table, args.dict_table, args.module)
    print("evidence: 1-row LookupWithString (bridge)")
    print(run_sql(args.ydb, args.endpoint, args.database, sql, stats=None), end="")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    if args.warmup < 1:
        raise SystemExit("warmup must be >= 1")
    if args.dict_from > args.dict_to:
        raise SystemExit("--dict-from must be <= --dict-to")

    module = "TrieNative" if args.native else "Trie"
    label = "%s::Lookup" % module
    n_dicts = args.dict_to - args.dict_from + 1
    print("measuring %s queries: dicts %s..%s (%s)" % (n_dicts, args.dict_from, args.dict_to, label))
    print("  warmup=%s (dict %s, excluded — compartment + compile)" % (args.warmup, args.dict_from))
    print(
        "  endpoint=%s db=%s addr=%s dict=%s"
        % (args.endpoint, args.database, args.addr_table, args.dict_table)
    )

    warmup_sql = sql_load(args.addr_table, args.dict_table, module, args.dict_from)
    for w in range(1, args.warmup + 1):
        timed_once(args.ydb, args.endpoint, args.database, warmup_sql)
        print("  warmup %s/%s done (excluded)" % (w, args.warmup))

    print()
    print("  %-6s %-8s %-12s %-14s %s" % ("dict", "size_mb", "wall_ms", "cpu_us", "cache"))
    samples: list[tuple[int, int, str]] = []
    cold = False
    for dict_id in range(args.dict_from, args.dict_to + 1):
        sql = sql_load(args.addr_table, args.dict_table, module, dict_id)
        wall_ms, cpu, cache = timed_once(args.ydb, args.endpoint, args.database, sql)
        samples.append((wall_ms, cpu, cache))
        if cache == "cold":
            cold = True
        print("  %-6s %-8s %-12s %-14s %s" % (dict_id, dict_id, wall_ms, cpu, cache))

    if cold:
        print(
            "warning: a measured run compiled from scratch (from_cache != true); "
            "median still includes it",
            file=sys.stderr,
        )

    med_ms = median_int([s[0] for s in samples])
    med_cpu = median_int([s[1] for s in samples])
    print()
    print(
        "addr=%s dict=%s dicts=%s..%s warmup=%s (excluded) (%s)"
        % (args.addr_table, args.dict_table, args.dict_from, args.dict_to, args.warmup, label)
    )
    print("  median: %6s ms wall, %8s us cpu" % (med_ms, med_cpu))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Trie UDF demo: build SQL and run")
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", "grpc://localhost:2146"))
    parser.add_argument("--database", default=os.environ.get("DB", "/Root/test"))
    parser.add_argument("--ydb", default=os.environ.get("YDB", "ydb"))
    parser.add_argument("--addr-table", default=os.environ.get("ADDR_TABLE", "ip_addr"))
    parser.add_argument("--dict-table", default=os.environ.get("DICT_TABLE", "ip_dict"))
    parser.add_argument("--module", default="Trie", help="YQL module for --readable/--evidence")
    parser.add_argument("--readable", action="store_true")
    parser.add_argument("--evidence", action="store_true")
    parser.add_argument("--readable-rows", type=int, default=10)
    parser.add_argument(
        "--dict-from",
        type=int,
        default=int(os.environ.get("DICT_FROM", "1")),
    )
    parser.add_argument(
        "--dict-to",
        type=int,
        default=int(os.environ.get("DICT_TO", "3")),
    )
    parser.add_argument("--warmup", type=int, default=int(os.environ.get("WARMUP", "1")))
    parser.add_argument(
        "--native",
        action="store_true",
        default=os.environ.get("NATIVE", "0") == "1",
        help="time TrieNative::Lookup instead of Trie",
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
