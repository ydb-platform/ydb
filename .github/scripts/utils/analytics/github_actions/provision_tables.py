#!/usr/bin/env python3
"""CREATE TABLE IF NOT EXISTS for analytics/ci_metrics and ci_metrics_state."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import _open_ydb_wrapper
from github_actions.ci_metrics import build_create_table_sql, resolve_table_path
from github_actions.state import build_create_state_table_sql, resolve_state_table_path


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create CI analytics tables if they do not exist")
    parser.add_argument("--metrics-table", default=None, help="Override the metrics table path")
    parser.add_argument("--state-table", default=None, help="Override the state table path")
    parser.add_argument("--skip-metrics", action="store_true", help="Do not touch the metrics table")
    parser.add_argument("--skip-state", action="store_true", help="Do not touch the state table")
    return parser.parse_args(argv)


def provision(
    *,
    metrics_table: Optional[str] = None,
    state_table: Optional[str] = None,
    skip_metrics: bool = False,
    skip_state: bool = False,
) -> int:
    if not has_send_credentials():
        print("Analytics YDB credentials are missing, cannot provision tables", file=sys.stderr)
        return 1
    with _open_ydb_wrapper() as wrapper:
        if not wrapper.check_credentials():
            print("Analytics YDB credentials are missing, cannot provision tables", file=sys.stderr)
            return 1
        if not skip_metrics:
            path = metrics_table or resolve_table_path(wrapper)
            wrapper.create_table(path, build_create_table_sql(path))
            print(f"Metrics table ready: {path}")
        if not skip_state:
            path = state_table or resolve_state_table_path(wrapper)
            wrapper.create_table(path, build_create_state_table_sql(path))
            print(f"State table ready: {path}")
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    return provision(
        metrics_table=args.metrics_table,
        state_table=args.state_table,
        skip_metrics=args.skip_metrics,
        skip_state=args.skip_state,
    )


if __name__ == "__main__":
    raise SystemExit(main())
