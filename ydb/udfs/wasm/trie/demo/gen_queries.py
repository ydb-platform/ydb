#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Emit readable / evidence SQL and one load query per dictionary.

Each load file is a separate statement: it materializes ONE dictionary by
id (scalar subquery, no JOIN) and scans every address.

For WASM Trie, use LookupPinned / LookupWithStringPinned (bridge CC):
guest BridgeRef + BridgeEnsureString pins the dict blob into compartment LM
once per handle. Native module keeps plain Lookup / LookupWithString.

Use --module TrieNative --suffix _native for the native baseline.
"""

from __future__ import annotations

import argparse
import os
import sys


def lookup_fn(module: str) -> str:
    return f"{module}::LookupPinned" if module == "Trie" else f"{module}::Lookup"


def lookup_with_string_fn(module: str) -> str:
    if module == "Trie":
        return f"{module}::LookupWithStringPinned"
    return f"{module}::LookupWithString"


def render_readable(addr_table: str, dict_table: str, limit: int, module: str) -> str:
    if module == "Trie":
        header = (
            "-- First addresses looked up in dictionary id=1 (1 MiB). No JOIN: scalar subquery.\n"
            "-- Bridge LookupWithStringPinned: TryReuse handle + BridgeRef + BridgeEnsureString\n"
            "-- pins $dict into compartment linear memory once per query generation.\n"
        )
    else:
        header = (
            "-- First addresses looked up in dictionary id=1 (1 MiB). No JOIN: scalar subquery.\n"
            "-- {module} loaded via --udfs-dir; PreferWasm does not apply.\n"
        ).format(module=module)
    return """/* syntax version 1 */
{header}
$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = 1ul;

SELECT
    id,
    ip,
    {fn}(addr, $dict) AS org
FROM `{addr}`
WHERE id <= {limit}ul
ORDER BY id;
""".format(
        header=header,
        dict=dict_table,
        addr=addr_table,
        limit=limit,
        fn=lookup_with_string_fn(module),
    )


def render_load(addr_table: str, dict_table: str, dict_id: int, module: str) -> str:
    if module == "Trie":
        comment = (
            "-- Full scan of addresses against dictionary id={dict_id} ({dict_id} MiB).\n"
            "-- Scalar subquery, not a JOIN: $dict is pinned once via bridge EnsureString.\n"
        ).format(dict_id=dict_id)
    else:
        comment = (
            "-- Full scan of addresses against dictionary id={dict_id} ({dict_id} MiB).\n"
            "-- Scalar subquery; {module} via --udfs-dir (no PreferWasm).\n"
        ).format(dict_id=dict_id, module=module)
    return """/* syntax version 1 */
{comment}$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = {dict_id}ul;

SELECT SUM({fn}(addr, $dict)) AS checksum
FROM `{addr}`;
""".format(
        comment=comment,
        dict=dict_table,
        addr=addr_table,
        dict_id=dict_id,
        fn=lookup_fn(module),
    )


def render_evidence(addr_table: str, dict_table: str, module: str) -> str:
    return """/* syntax version 1 */
-- One address, dictionary id=1. Bridge pin path (no RFC 005 pragmas).
$dict = SELECT Unwrap(MIN(acl)) FROM `{dict}` WHERE id = 1ul;

SELECT id, ip,
       {fn}(addr, $dict) AS org
FROM `{addr}`
WHERE id = 1ul;
""".format(
        dict=dict_table,
        addr=addr_table,
        fn=lookup_with_string_fn(module),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Trie IP demo SQL files")
    parser.add_argument("--out-dir", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--addr-table", default="ip_addr")
    parser.add_argument("--dict-table", default="ip_dict")
    parser.add_argument("--readable-rows", type=int, default=10)
    parser.add_argument("--dicts", type=int, default=10, help="emit demo_load_01.sql .. demo_load_N.sql")
    parser.add_argument("--module", default="Trie", help="YQL module prefix (Trie or TrieNative)")
    parser.add_argument("--suffix", default="", help="filename suffix, e.g. _native")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    files = {}
    if not args.suffix:
        files["demo_readable.sql"] = render_readable(
            args.addr_table, args.dict_table, args.readable_rows, args.module
        )
        files["demo_evidence.sql"] = render_evidence(
            args.addr_table, args.dict_table, args.module
        )
    for dict_id in range(1, args.dicts + 1):
        name = "demo_load_{:02d}{}.sql".format(dict_id, args.suffix)
        files[name] = render_load(args.addr_table, args.dict_table, dict_id, args.module)

    for name, body in files.items():
        path = os.path.join(args.out_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        print("wrote", path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
