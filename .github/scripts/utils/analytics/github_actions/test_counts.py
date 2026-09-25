"""Count pass/fail/skip/muted from a ya build-results report (after mute transform)."""

from __future__ import annotations

import json
from typing import Dict

COUNT_NAMES = ("passed", "failed", "errors", "skipped", "muted", "total")


def count_report_tests(path: str) -> Dict[str, int]:
    with open(path, encoding="utf-8") as handle:
        report = json.load(handle)
    counts = {name: 0 for name in COUNT_NAMES}
    for result in report.get("results") or []:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").upper()
        if status in ("PASSED", "OK"):
            counts["passed"] += 1
        elif status == "FAILED":
            counts["failed"] += 1
        elif status == "ERROR":
            counts["errors"] += 1
        elif status == "SKIPPED":
            counts["skipped"] += 1
        elif status == "MUTE":
            counts["muted"] += 1
        else:
            continue
        counts["total"] += 1
    return counts


