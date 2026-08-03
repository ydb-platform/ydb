#!/usr/bin/env python3
"""Export filtered LCOV from a ya --coverage-report output directory.

Uses coverage.report/coverage.profdata and the llvm-cov object list from
build_clang_coverage_report.log (same sources ya used for the HTML report).
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from codecov_suites import COVERAGE_EXCLUDE_REGEXP, SUITES

EXCLUDE_RE = re.compile(COVERAGE_EXCLUDE_REGEXP)

# ya logs: Executing: ['.../bin/llvm-cov', 'export', ..., '-object', 'path', ...]
EXECUTING_RE = re.compile(r"Executing:\s*(\[.*bin/llvm-cov',\s*'export'.*\])")


def parse_ya_llvm_cov_cmd(log_text: str) -> tuple[str, list[str]]:
    """Return (llvm_cov_path, object_paths) from ya coverage report log."""
    match = EXECUTING_RE.search(log_text)
    if not match:
        # Fallback: multiline / slightly different quoting
        match = re.search(
            r"(\[[^\]]*bin/llvm-cov',\s*'export'[^\]]*\])",
            log_text,
            re.S,
        )
    if not match:
        raise SystemExit("Could not find llvm-cov export command in coverage log")

    try:
        cmd = ast.literal_eval(match.group(1))
    except (SyntaxError, ValueError) as exc:
        raise SystemExit(f"Failed to parse llvm-cov command: {exc}") from exc

    if not cmd or "llvm-cov" not in cmd[0]:
        raise SystemExit(f"Unexpected llvm-cov command: {cmd[:3]!r}")

    llvm_cov = cmd[0]
    objects: list[str] = []
    # First positional after flags is often the main binary; also collect -object=
    i = 1
    while i < len(cmd):
        arg = cmd[i]
        if arg == "-object" and i + 1 < len(cmd):
            objects.append(cmd[i + 1])
            i += 2
            continue
        if arg.startswith("-object="):
            objects.append(arg.split("=", 1)[1])
            i += 1
            continue
        if arg.startswith("-"):
            i += 1
            continue
        # positional binary
        if "instr-profile" not in arg and arg not in ("export", "show"):
            objects.append(arg)
        i += 1

    # Deduplicate preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for o in objects:
        if o not in seen:
            seen.add(o)
            uniq.append(o)

    if not uniq:
        raise SystemExit("No -object binaries found in llvm-cov command")
    return llvm_cov, uniq


def path_matches_prefixes(path: str, prefixes: list[str]) -> bool:
    norm = path.replace("\\", "/")
    for pref in prefixes:
        p = pref.rstrip("/")
        if (
            f"/{p}/" in f"/{norm}/"
            or norm.startswith(pref)
            or norm.endswith("/" + p)
            or p in norm
        ):
            # Prefer startswith-style match on repo-relative segment
            idx = norm.find(p)
            if idx != -1 and (idx == 0 or norm[idx - 1] == "/"):
                return True
    return False


def filter_lcov(text: str, prefixes: list[str]) -> str:
    records: list[str] = []
    current: list[str] = []
    keep = False

    def flush() -> None:
        nonlocal current, keep
        if current and keep:
            records.append("".join(current))
        current = []
        keep = False

    for line in text.splitlines(keepends=True):
        if line.startswith("TN:"):
            flush()
            current = [line]
            keep = False
            continue
        if line.startswith("SF:"):
            if not current:
                current = ["TN:\n"]
            path = line[3:].strip()
            keep = path_matches_prefixes(path, prefixes) and not EXCLUDE_RE.search(
                path.replace("\\", "/")
            )
            current.append(line)
            continue
        if line.startswith("end_of_record"):
            current.append(line)
            flush()
            continue
        if current:
            current.append(line)
    flush()
    return "".join(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=sorted(SUITES.keys()))
    parser.add_argument(
        "--ya-output",
        required=True,
        type=Path,
        help="Directory passed to ya make --output (contains coverage.report/)",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Override path to build_clang_coverage_report.log",
    )
    parser.add_argument(
        "--profdata",
        type=Path,
        default=None,
        help="Override path to coverage.profdata",
    )
    parser.add_argument("--llvm-cov", default="", help="Override llvm-cov binary")
    args = parser.parse_args()

    cfg = SUITES[args.suite]
    ya_out = args.ya_output.resolve()
    report_dir = ya_out / "coverage.report"
    profdata = args.profdata or (report_dir / "coverage.profdata")
    log_path = args.log or (ya_out / "build_clang_coverage_report.log")

    if not profdata.is_file():
        raise SystemExit(f"Missing profdata: {profdata}")
    if not log_path.is_file():
        raise SystemExit(f"Missing coverage log: {log_path}")

    log_text = log_path.read_text(encoding="utf-8", errors="replace")
    llvm_cov, objects = parse_ya_llvm_cov_cmd(log_text)
    if args.llvm_cov:
        llvm_cov = args.llvm_cov

    missing = [o for o in objects if not Path(o).is_file()]
    if missing:
        print(f"Warning: {len(missing)}/{len(objects)} binaries missing", file=sys.stderr)
        for m in missing[:10]:
            print(f"  missing: {m}", file=sys.stderr)
        objects = [o for o in objects if Path(o).is_file()]
    if not objects:
        raise SystemExit("No instrumented binaries available for llvm-cov export")

    cmd = [
        llvm_cov,
        "export",
        "-format=lcov",
        f"-instr-profile={profdata}",
        objects[0],
    ]
    for obj in objects[1:]:
        cmd.append(f"-object={obj}")

    print("+", " ".join(cmd[:6]), f"... ({len(objects)} objects)", flush=True)
    proc = subprocess.run(cmd, check=True, text=True, capture_output=True)
    filtered = filter_lcov(proc.stdout, cfg["lcov_prefixes"])
    if not filtered.strip():
        print("Warning: filtered LCOV is empty", file=sys.stderr)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(filtered, encoding="utf-8")
    n_rec = filtered.count("end_of_record")
    print(f"Wrote {args.output} ({len(filtered)} bytes, {n_rec} records)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
