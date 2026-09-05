#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Emit readable / evidence SQL and one load query per (table, shape).

Shapes:
  probes  — K distinct Module::byte_at (O(1); host copies the blob K times)
  letters — Module::count_letters (O(n) body, one call; copy often lost in noise)
  multi   — letters + digits + upper (three O(n) exports)
  length  — Module::text_length (O(1) body, one call)
"""

from __future__ import annotations

import argparse
import os
import sys

TABLES = ("text_200kb", "text_1mb", "text_2mb")
PROBE_COUNT = 16


def probes_projection(module: str, k: int) -> str:
    terms = ["%s::byte_at(txt, %s)" % (module, i) for i in range(k)]
    return "SUM(" + " + ".join(terms) + ")"


def make_shapes(module: str, probe_count: int) -> dict[str, str]:
    return {
        "probes": probes_projection(module, probe_count),
        "letters": "SUM(%s::count_letters(txt))" % module,
        "multi": (
            "SUM(%s::count_letters(txt)"
            " + %s::count_digits(txt)"
            " + %s::count_upper(txt))" % (module, module, module)
        ),
        "length": "SUM(%s::text_length(txt))" % module,
    }


def render_load(table: str, projection: str, module: str) -> str:
    if module == "Text":
        comment = (
            "-- Full scan of `{table}`. SUM without ORDER BY keeps the UDF in the same\n"
            "-- stage as the table read so CollectWasmUdfStringColumns marks `txt`.\n"
        ).format(table=table)
    else:
        comment = (
            "-- Full scan of `{table}`. SUM without ORDER BY; {module} loaded via --udfs-dir.\n"
        ).format(table=table, module=module)
    return """/* syntax version 1 */
{comment}SELECT {projection} AS checksum
FROM `{table}`;
""".format(table=table, projection=projection, comment=comment)


def render_readable(table: str, limit: int, module: str) -> str:
    return """/* syntax version 1 */
-- First rows of `{table}` with counters. PreferWasm on.
PRAGMA ydb.EnableWasmUdfResidentStringColumns = "true";

SELECT
    id,
    SUBSTRING(txt, 0u, 48u) AS head,
    {module}::count_letters(txt) AS letters,
    {module}::count_digits(txt) AS digits,
    {module}::count_upper(txt) AS upper,
    {module}::text_length(txt) AS len,
    {module}::byte_at(txt, 0) AS b0
FROM `{table}`
WHERE id <= {limit}ul
ORDER BY id;
""".format(table=table, limit=limit, module=module)


def render_evidence(table: str, module: str) -> str:
    return """/* syntax version 1 */
-- One row. Use with YDB_WASM_STRING_DEBUG=1 on the tenant node.
SELECT id,
       {module}::count_letters(txt) AS letters,
       {module}::byte_at(txt, 0) AS b0,
       {module}::text_length(txt) AS len
FROM `{table}`
WHERE id = 1ul;
""".format(table=table, module=module)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Text UDF demo SQL files")
    parser.add_argument("--out-dir", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--readable-rows", type=int, default=5)
    parser.add_argument("--readable-table", default="text_200kb")
    parser.add_argument("--probes", type=int, default=PROBE_COUNT)
    parser.add_argument("--module", default="Text", help="YQL module prefix (Text or TextNative)")
    parser.add_argument("--suffix", default="", help="filename suffix, e.g. _native")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    shapes = make_shapes(args.module, args.probes)
    files = {}
    if not args.suffix:
        files["demo_readable.sql"] = render_readable(args.readable_table, args.readable_rows, args.module)
        files["demo_evidence.sql"] = render_evidence(args.readable_table, args.module)
    for table in TABLES:
        for shape, projection in shapes.items():
            name = "demo_%s_%s%s.sql" % (table, shape, args.suffix)
            files[name] = render_load(table, projection, args.module)
    for name, text in files.items():
        path = os.path.join(args.out_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        print("wrote %s" % path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
