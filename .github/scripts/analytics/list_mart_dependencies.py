#!/usr/bin/env python3
"""List mart SQL read dependencies and workflow write targets.

Helper to refresh the «Mart SQL registry» table in ARCHITECTURE.md:

  python3 .github/scripts/analytics/list_mart_dependencies.py
  python3 .github/scripts/analytics/list_mart_dependencies.py --markdown
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
QUERIES_DIR = REPO_ROOT / ".github/scripts/analytics/data_mart_queries"
WORKFLOW = REPO_ROOT / ".github/workflows/collect_analytics_fast.yml"

TABLE_REF = re.compile(r"(?:FROM|JOIN)\s+`([^`]+)`", re.IGNORECASE)
EXECUTOR_LINE = re.compile(
    r"data_mart_executor\.py\s+.*--query_path\s+(\S+\.sql)\s+.*--table_path\s+(\S+)"
)


def extract_reads(sql_path: Path) -> list[str]:
    text = sql_path.read_text(encoding="utf-8")
    return sorted(set(TABLE_REF.findall(text)))


def parse_workflow_writes() -> dict[str, str]:
    writes: dict[str, str] = {}
    for line in WORKFLOW.read_text(encoding="utf-8").splitlines():
        match = EXECUTOR_LINE.search(line)
        if not match:
            continue
        query_rel = match.group(1)
        if query_rel.startswith(".github/"):
            key = Path(query_rel).name
        else:
            key = Path(query_rel).name
        writes[key] = match.group(2)
    return writes


def collect_rows() -> list[tuple[str, str, str]]:
    writes = parse_workflow_writes()
    rows: list[tuple[str, str, str]] = []
    for sql_path in sorted(QUERIES_DIR.rglob("*.sql")):
        if "datalens_ds_queries" in sql_path.parts:
            continue
        rel = sql_path.relative_to(REPO_ROOT).as_posix()
        reads = ", ".join(extract_reads(sql_path)) or "—"
        write = writes.get(sql_path.name, "— (manual / other workflow)")
        rows.append((rel, write, reads))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Print markdown table rows for ARCHITECTURE.md",
    )
    args = parser.parse_args()

    rows = collect_rows()
    if args.markdown:
        print("| SQL | Writes (`--table_path`) | Reads (`FROM`/`JOIN`) |")
        print("|-----|-------------------------|------------------------|")
        for sql, write, reads in rows:
            print(f"| `{sql}` | `{write}` | {reads} |")
        return

    for sql, write, reads in rows:
        print(f"{sql}\n  writes: {write}\n  reads:  {reads}\n")


if __name__ == "__main__":
    main()
