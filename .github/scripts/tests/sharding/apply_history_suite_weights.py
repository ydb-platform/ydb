#!/usr/bin/env python3
"""Replace size-based suite weights with YDB suite duration p50 when history exists."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_TESTS_DIR = _SCRIPT_DIR.parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

from extract_suites_from_ya_test_list import parse_ya_test_list  # noqa: E402
from get_test_duration_estimates import DEFAULT_DAYS_BACK, get_suite_duration_p50  # noqa: E402


def apply_history_weights(
    summary: dict,
    *,
    list_log: Path,
    target_prefix: str | None,
    allowed_sizes: set[str] | None,
    build_type: str,
    branch: str,
    days_back: int,
) -> dict:
    suites, _, _ = parse_ya_test_list(
        list_log.read_text(encoding="utf-8"),
        target_prefix=target_prefix,
        allowed_sizes=allowed_sizes,
    )
    suite_paths = [suite.path for suite in suites]

    try:
        duration_p50 = get_suite_duration_p50(suite_paths, days_back, build_type, branch)
    except Exception as exc:
        print(f"warning: duration history lookup failed: {exc}", file=sys.stderr)
        duration_p50 = {}

    suite_meta: dict[str, dict] = {}
    history_suites = 0
    size_fallback_suites = 0
    total_weight = 0.0

    for suite in suites:
        if suite.path in duration_p50:
            weight = duration_p50[suite.path]
            source = "history"
            history_suites += 1
            history_tests = suite.test_count
            size_fallback_tests = 0
        else:
            weight = float(suite.weight)
            source = "size"
            size_fallback_suites += 1
            history_tests = 0
            size_fallback_tests = suite.test_count
        total_weight += weight
        suite_meta[suite.path] = {
            "weight": weight,
            "weight_source": source,
            "history_test_count": history_tests,
            "size_fallback_test_count": size_fallback_tests,
            "history_p50_sec": duration_p50.get(suite.path),
        }

    updated_suites = []
    for suite in summary.get("suites") or []:
        path = str(suite.get("path") or "").strip()
        item = dict(suite)
        meta = suite_meta.get(path)
        if meta:
            item["weight"] = meta["weight"]
            item["weight_source"] = meta["weight_source"]
            item["history_test_count"] = meta["history_test_count"]
            item["size_fallback_test_count"] = meta["size_fallback_test_count"]
            if meta.get("history_p50_sec") is not None:
                item["history_p50_sec"] = meta["history_p50_sec"]
        updated_suites.append(item)

    summary = dict(summary)
    summary["suites"] = updated_suites
    summary["total_weight"] = total_weight
    summary["weighting"] = {
        "mode": "suite_history_p50_with_size_fallback",
        "build_type": build_type,
        "branch": branch,
        "days_back": days_back,
        "history_suite_count": history_suites,
        "size_fallback_suite_count": size_fallback_suites,
        "history_test_count": sum(
            suite.test_count for suite in suites if suite.path in duration_p50
        ),
        "size_fallback_test_count": sum(
            suite.test_count for suite in suites if suite.path not in duration_p50
        ),
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("list_log", type=Path, help="ya test -L output used for the plan")
    parser.add_argument("summary_json", type=Path, help="list_summary.json to update in place")
    parser.add_argument("--target-prefix", default="ydb/")
    parser.add_argument(
        "--test-size",
        action="append",
        default=["small", "medium"],
        help="Test sizes included in the plan (default: small+medium)",
    )
    parser.add_argument("--build-type", default="relwithdebinfo")
    parser.add_argument("--branch", default="main")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    args = parser.parse_args()

    if not args.list_log.is_file():
        print(f"error: missing list log: {args.list_log}", file=sys.stderr)
        return 2
    if not args.summary_json.is_file():
        print(f"error: missing summary json: {args.summary_json}", file=sys.stderr)
        return 2

    allowed_sizes = {size.strip().lower() for size in args.test_size if size.strip()}
    summary = json.loads(args.summary_json.read_text(encoding="utf-8"))
    updated = apply_history_weights(
        summary,
        list_log=args.list_log,
        target_prefix=args.target_prefix,
        allowed_sizes=allowed_sizes or None,
        build_type=args.build_type,
        branch=args.branch,
        days_back=args.days_back,
    )
    args.summary_json.write_text(json.dumps(updated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    weighting = updated.get("weighting") or {}
    print(
        f"Updated suite weights: history_suites={weighting.get('history_suite_count', 0)}, "
        f"size_fallback_suites={weighting.get('size_fallback_suite_count', 0)}, "
        f"total_weight={updated.get('total_weight')}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
