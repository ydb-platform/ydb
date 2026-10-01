#!/usr/bin/env python3
"""Export suite-filtered LCOV and, optionally, a matching HTML report.

Uses coverage.report/coverage.profdata and the llvm-cov object list from
build_clang_coverage_report.log. Both outputs contain only source files owned
by the selected suite; transitive dependencies may be executed, but they do
not contribute to the component coverage percentage.
"""

from __future__ import annotations

import argparse
import ast
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from codecov_suites import COVERAGE_EXCLUDE_REGEXP, SUITES

EXCLUDE_RE = re.compile(COVERAGE_EXCLUDE_REGEXP)

# ya logs: Executing: ['.../bin/llvm-cov', 'export', ..., '-object', 'path', ...]
EXECUTING_RE = re.compile(r"Executing:\s*(\[.*bin/llvm-cov',\s*'export'.*\])")

# llvm-cov options that consume the following argv token (not binaries).
# Store normalized names because LLVM accepts both -option and --option forms.
_LLVM_COV_VALUE_FLAGS = frozenset(
    {
        "instr-profile",
        "object",
        "format",
        "ignore-filename-regex",
        "include-filename-regex",
        "name",
        "name-regex",
        "name-allowlist",
        "name-whitelist",
        "path-equivalence",
        "j",
        "num-threads",
        "Xdemangler",
        "coverage-watermark",
        "arch",
        "tab-size",
        "output-dir",
        "compilation-dir",
        "debug-file-directory",
        "line-coverage-gt",
        "line-coverage-lt",
        "region-coverage-gt",
        "region-coverage-lt",
    }
)


def parse_ya_llvm_cov_cmd(log_text: str) -> tuple[str, list[str]]:
    """Return (llvm_cov_path, object_paths) from ya coverage report log.

    Only real instrumented binaries are returned. Values of flags such as
    -instr-profile / -ignore-filename-regex must not be treated as -object paths
    (that bug made llvm-cov export fail with exit 1 in CI).
    """
    match = EXECUTING_RE.search(log_text)
    if not match:
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
    i = 1
    while i < len(cmd):
        arg = cmd[i]
        if arg in ("export", "show", "report"):
            i += 1
            continue
        if arg.startswith("-"):
            option, separator, value = arg.lstrip("-").partition("=")
            if option == "object":
                if separator:
                    objects.append(value)
                    i += 1
                elif i + 1 < len(cmd):
                    objects.append(cmd[i + 1])
                    i += 2
                else:
                    raise SystemExit(f"Missing value for llvm-cov option: {arg}")
                continue
            if option in _LLVM_COV_VALUE_FLAGS:
                if separator:
                    i += 1
                elif i + 1 < len(cmd):
                    i += 2
                else:
                    raise SystemExit(f"Missing value for llvm-cov option: {arg}")
                continue
            # Unknown boolean option.
            i += 1
            continue
        # Positional binary (legacy llvm-cov style): path, not .profdata
        if "/" in arg and not arg.endswith((".profdata", ".profraw")):
            objects.append(arg)
        i += 1

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
    """True if path is under any prefix as a path segment (not a mere substring)."""
    norm = path.replace("\\", "/")
    for pref in prefixes:
        p = pref.rstrip("/")
        if not p:
            continue
        idx = 0
        while True:
            idx = norm.find(p, idx)
            if idx == -1:
                break
            end = idx + len(p)
            left_ok = idx == 0 or norm[idx - 1] == "/"
            right_ok = end == len(norm) or norm[end] == "/"
            if left_ok and right_ok:
                return True
            idx = end
    return False


def filter_lcov(
    text: str,
    prefixes: list[str],
    *,
    excluded_sources: set[str] | frozenset[str] = frozenset(),
) -> str:
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
            keep = keep and path not in excluded_sources
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


def lcov_sources(text: str) -> list[str]:
    """Return unique source paths from a filtered LCOV stream."""
    seen: set[str] = set()
    sources: list[str] = []
    for line in text.splitlines():
        if not line.startswith("SF:"):
            continue
        source = line[3:].strip()
        if source and source not in seen:
            seen.add(source)
            sources.append(source)
    return sources


def is_ephemeral_ya_build_source(path: str) -> bool:
    """True for generated sources inside ya's disposable build root."""
    return "/.ya/build/build_root/" in path.replace("\\", "/")


def filter_suite_lcov(text: str, prefixes: list[str]) -> tuple[str, list[str]]:
    """Keep checked-in suite sources and return discarded ya-generated paths."""
    owned = filter_lcov(text, prefixes)
    generated_sources = [
        source for source in lcov_sources(owned) if is_ephemeral_ya_build_source(source)
    ]
    filtered = filter_lcov(
        owned,
        prefixes,
        excluded_sources=frozenset(generated_sources),
    )
    missing_sources = [source for source in lcov_sources(filtered) if not Path(source).is_file()]
    if missing_sources:
        print(
            f"Error: {len(missing_sources)} checked-in source file(s) missing",
            file=sys.stderr,
        )
        for source in missing_sources[:10]:
            print(f"  missing source: {source}", file=sys.stderr)
        raise SystemExit("Refusing to publish LCOV with missing source files")
    return filtered, generated_sources


def generate_html_report(
    llvm_cov: str,
    objects: list[str],
    profdata: Path,
    filtered_lcov: str,
    output_dir: Path,
) -> None:
    """Generate HTML for exactly the source files retained in filtered LCOV."""
    sources = lcov_sources(filtered_lcov)
    if not sources:
        raise SystemExit("Filtered LCOV contains no source files for the HTML report")
    missing_sources = [source for source in sources if not Path(source).is_file()]
    if missing_sources:
        print(
            f"Error: {len(missing_sources)}/{len(sources)} filtered source files missing",
            file=sys.stderr,
        )
        for source in missing_sources[:10]:
            print(f"  missing source: {source}", file=sys.stderr)
        raise SystemExit("Refusing to generate HTML from an incomplete source set")
    if output_dir.exists() or output_dir.is_symlink():
        raise SystemExit(f"Refusing to replace an existing HTML output path: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent)
    )

    cmd = [
        llvm_cov,
        "show",
        "-format=html",
        f"-output-dir={temporary_dir}",
        f"-instr-profile={profdata}",
        objects[0],
    ]
    for obj in objects[1:]:
        cmd.append(f"-object={obj}")
    # The explicit source list is the important part: llvm-cov otherwise emits
    # every dependency source present in the instrumented object closure.
    cmd.append("-sources")
    cmd.extend(sources)

    print(
        "+",
        " ".join(cmd[:6]),
        f"... ({len(objects)} objects, {len(sources)} sources)",
        flush=True,
    )
    try:
        proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
        if proc.returncode != 0:
            print(proc.stdout, file=sys.stderr)
            print(proc.stderr, file=sys.stderr)
            raise SystemExit(f"llvm-cov HTML generation failed with exit {proc.returncode}")
        if not (temporary_dir / "index.html").is_file():
            raise SystemExit(
                f"llvm-cov did not create the HTML index: {temporary_dir / 'index.html'}"
            )
        temporary_dir.replace(output_dir)
    finally:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, choices=sorted(SUITES.keys()))
    parser.add_argument(
        "--ya-output",
        required=True,
        type=Path,
        help="Directory passed to ya make --output (contains coverage.report/)",
    )
    parser.add_argument("--output", type=Path, default=None, help="Filtered LCOV output path")
    parser.add_argument(
        "--html-input",
        type=Path,
        default=None,
        help="Existing filtered LCOV used as the exact HTML source list",
    )
    parser.add_argument(
        "--html-output",
        type=Path,
        default=None,
        help="Generate an llvm-cov HTML report for the same filtered source set",
    )
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

    if args.html_input is not None:
        if args.output is not None or args.html_output is None:
            parser.error("--html-input requires --html-output and cannot be combined with --output")
    elif args.output is None or args.html_output is not None:
        parser.error("LCOV export requires --output only; use --html-input for HTML")

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
        print(f"Error: {len(missing)}/{len(objects)} instrumented binaries missing", file=sys.stderr)
        for m in missing[:10]:
            print(f"  missing: {m}", file=sys.stderr)
        raise SystemExit("Refusing to publish incomplete coverage")

    if args.html_input is not None:
        if not args.html_input.is_file():
            raise SystemExit(f"Missing filtered LCOV input: {args.html_input}")
        filtered = args.html_input.read_text(encoding="utf-8")
        sources = lcov_sources(filtered)
        if not sources:
            raise SystemExit("Filtered LCOV contains no sources; refusing to generate HTML")
        unexpected = [
            source
            for source in sources
            if not path_matches_prefixes(source, cfg["lcov_prefixes"])
            or EXCLUDE_RE.search(source.replace("\\", "/"))
            or is_ephemeral_ya_build_source(source)
        ]
        if unexpected:
            raise SystemExit(
                f"Filtered LCOV contains {len(unexpected)} source(s) outside suite {args.suite}"
            )
        generate_html_report(llvm_cov, objects, profdata, filtered, args.html_output)
        print(f"Wrote filtered HTML report to {args.html_output}")
        return 0

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
    proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(f"llvm-cov export failed with exit {proc.returncode}")

    filtered, generated_sources = filter_suite_lcov(proc.stdout, cfg["lcov_prefixes"])
    if generated_sources:
        print(
            f"Warning: dropping {len(generated_sources)} ephemeral ya-generated source file(s) from LCOV",
            file=sys.stderr,
        )
        for source in generated_sources[:10]:
            print(f"  dropped source: {source}", file=sys.stderr)
    if not filtered.strip():
        raise SystemExit(
            f"Filtered LCOV is empty for suite {args.suite}; refusing to upload a false 0% report"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(filtered, encoding="utf-8")
    n_rec = filtered.count("end_of_record")
    print(f"Wrote {args.output} ({len(filtered)} bytes, {n_rec} records)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
