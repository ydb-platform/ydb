"""Count test results from a ya build-results report (after mute transform)."""

from __future__ import annotations

import json
from typing import Dict

COUNT_NAMES = ("passed", "failed", "errors", "skipped", "muted", "not_launched", "other", "total")


def count_report_tests(path: str) -> Dict[str, int]:
    with open(path, encoding="utf-8") as handle:
        report = json.load(handle)
    counts = {name: 0 for name in COUNT_NAMES}
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").upper()
        error_type = str(result.get("error_type") or "").upper()
        if not status:
            continue
        if status in ("PASSED", "OK"):
            bucket = "passed"
        elif status == "FAILED":
            bucket = "failed"
        elif status == "ERROR":
            bucket = "errors"
        elif status == "NOT_LAUNCHED" or error_type == "NOT_LAUNCHED":
            bucket = "not_launched"
        elif status == "SKIPPED":
            bucket = "skipped"
        elif status == "MUTE":
            bucket = "muted"
        else:
            bucket = "other"
        counts[bucket] += 1
        counts["total"] += 1
    return counts


