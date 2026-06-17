#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path

TOLERANCE = 0.1  # percentage points


def line_pct(report_dir: str) -> float:
    html = (Path(report_dir) / "index.html").read_text(encoding="utf-8", errors="replace")
    match = re.search(r"Lines:</td>\s*<td[^>]*>([\d.]+)\s*%", html, re.I)
    if not match:
        raise SystemExit(f"line coverage not found in {report_dir}/index.html")
    return float(match.group(1))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: compare_cpp_sdk_coverage.py {extract|check} ...")

    cmd = sys.argv[1]
    if cmd == "extract":
        if len(sys.argv) != 4:
            raise SystemExit("usage: compare_cpp_sdk_coverage.py extract <report_dir> <out_json>")
        with open(sys.argv[3], "w", encoding="utf-8") as f:
            json.dump({"line_pct": line_pct(sys.argv[2])}, f)
    elif cmd == "check":
        if len(sys.argv) != 4:
            raise SystemExit("usage: compare_cpp_sdk_coverage.py check <current_json> <baseline_json>")
        with open(sys.argv[2], encoding="utf-8") as f:
            current = json.load(f)["line_pct"]
        with open(sys.argv[3], encoding="utf-8") as f:
            baseline = json.load(f)["line_pct"]
        print(
            f"CPP SDK line coverage: {current}% (baseline: {baseline}%, tolerance: {TOLERANCE}pp)",
            file=sys.stderr,
        )
        sys.exit(1 if current < baseline - TOLERANCE else 0)
    else:
        raise SystemExit(f"unknown command: {cmd}")
