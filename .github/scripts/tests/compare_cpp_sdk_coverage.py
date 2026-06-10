#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path


def line_pct(report_dir: str) -> float:
    html = (Path(report_dir) / "index.html").read_text(encoding="utf-8", errors="replace")
    match = re.search(r"Lines:</td>\s*<td[^>]*>([\d.]+)\s*%", html, re.I)
    if not match:
        raise SystemExit(f"line coverage not found in {report_dir}/index.html")
    return float(match.group(1))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "extract":
        json.dump({"line_pct": line_pct(sys.argv[2])}, open(sys.argv[3], "w"))
    elif cmd == "check":
        current = json.load(open(sys.argv[2]))["line_pct"]
        baseline = json.load(open(sys.argv[3]))["line_pct"]
        print(f"CPP SDK line coverage: {current}% (main: {baseline}%)", file=sys.stderr)
        sys.exit(1 if current < baseline else 0)
    else:
        raise SystemExit(f"unknown command: {cmd}")
