#!/usr/bin/env python3
#
# Compare vector/fulltext workload performance between main branch and current branch.
#
# The main branch ydbd is downloaded as a prebuilt binary from S3 (ydb-builds bucket).
# The current branch ydbd is built from the local source tree.
#
# IMPORTANT: Both binaries must use the same build preset for a fair comparison.
# The S3 bucket has builds for: release, relwithdebinfo.
#
# Usage:
#   ./ydb/tests/stress/compare_index_performance.py [OPTIONS]
#
# Defaults are for quick local runs. The GitHub workflow uses larger values
# (duration=600, iterations=20) for CI.
#
# The per-workload knobs are forwarded to the inner workload tests
# (ydb/tests/stress/{vector,fulltext}_workload/tests) via `ya make --test-param`.
#
# Options:
#   --duration SECONDS     Duration of each workload run (default: 60)
#   --build-preset PRESET  Build preset for both binaries (default: relwithdebinfo)
#   --main-ydbd PATH       Path to pre-built main branch ydbd (skips downloading)
#   --current-ydbd PATH    Path to pre-built current branch ydbd (skips building)
#   --ref REF              S3 ref to download ydbd from (default: main)
#   --workload WORKLOAD    Run only specified workload: vector, fulltext, or all (default: all)
#   --targets N            Number of query vectors for vector select (default: 1000)
#   --iterations N         Number of iterations per workload (default: 3)
#   --warmup SECONDS       Warmup duration before each measured run (default: 30)
#   --rows N               Number of rows in generated database (default: 10000)
#   --threads N            Number of threads for load testing (default: 10)
#   --main-feature-flag FLAG     Enable a feature flag for main branch (repeatable)
#   --current-feature-flag FLAG  Enable a feature flag for current branch (repeatable)
#   --main-table-service-config KEY=VALUE     Set table_service_config option for main branch (repeatable)
#   --current-table-service-config KEY=VALUE  Set table_service_config option for current branch (repeatable)

import argparse
import os
import shutil
import statistics
import subprocess
import sys
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent
S3_BASE_URL = "https://storage.yandexcloud.net/ydb-builds"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare vector/fulltext workload performance between main and current branch.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration of each workload run (default: 60)")
    parser.add_argument("--build-preset", default="relwithdebinfo",
                        help="Build preset for both binaries (default: relwithdebinfo)")
    parser.add_argument("--main-ydbd", default="",
                        help="Path to pre-built main branch ydbd (skips downloading)")
    parser.add_argument("--current-ydbd", default="",
                        help="Path to pre-built current branch ydbd (skips building)")
    parser.add_argument("--ref", dest="s3_ref", default="main",
                        help="S3 ref to download ydbd from (default: main)")
    parser.add_argument("--workload", default="all", choices=["all", "vector", "fulltext"],
                        help="Run only specified workload: vector, fulltext, or all (default: all)")
    parser.add_argument("--targets", type=int, default=1000,
                        help="Number of query vectors for vector select (default: 1000)")
    parser.add_argument("--iterations", type=int, default=3,
                        help="Number of iterations per workload (default: 3)")
    parser.add_argument("--warmup", type=int, default=30,
                        help="Warmup duration before each measured run (default: 30)")
    parser.add_argument("--rows", type=int, default=10000,
                        help="Number of rows in generated database (default: 10000)")
    parser.add_argument("--threads", type=int, default=10,
                        help="Number of threads for load testing (default: 10)")
    parser.add_argument("--main-feature-flag", action="append", default=[],
                        help="Enable a feature flag for main branch (repeatable)")
    parser.add_argument("--current-feature-flag", action="append", default=[],
                        help="Enable a feature flag for current branch (repeatable)")
    parser.add_argument("--main-table-service-config", action="append", default=[],
                        help="Set table_service_config option for main branch (repeatable)")
    parser.add_argument("--current-table-service-config", action="append", default=[],
                        help="Set table_service_config option for current branch (repeatable)")
    return parser.parse_args()


# --- Statistics helpers ---
def median(vals):
    if not vals:
        return "N/A"
    return statistics.median(vals)


def mean(vals):
    if not vals:
        return "N/A"
    return statistics.fmean(vals)


def stddev(vals):
    # Sample standard deviation
    if len(vals) < 2:
        return 0.0
    return statistics.stdev(vals)


def calc_diff(main_val, current_val):
    if main_val in ("N/A", None, "") or current_val in ("N/A", None, ""):
        return "N/A"
    main_val = float(main_val)
    current_val = float(current_val)
    if main_val == 0:
        return "N/A (zero base)"
    diff = (current_val - main_val) / main_val * 100
    return "%+.1f%%" % diff


def calc_significance(main_mean, current_mean, main_stddev, current_stddev, main_n, current_n):
    # Check statistical significance using 3-sigma rule on the difference of means.
    # Uses pooled standard error: SE = sqrt(s1^2/n1 + s2^2/n2)
    if main_mean in ("N/A", None) or current_mean in ("N/A", None):
        return "N/A"
    if main_n < 2 or current_n < 2:
        return "N/A (need >= 2 iterations)"

    se = (main_stddev ** 2 / main_n + current_stddev ** 2 / current_n) ** 0.5
    diff = current_mean - main_mean
    absdiff = abs(diff)
    threshold = 3 * se
    pct = (diff / main_mean * 100) if main_mean != 0 else 0
    if threshold > 0 and absdiff > threshold:
        return "SIGNIFICANT (%+.1f%%, |diff|=%.1f > 3sigma=%.1f)" % (pct, absdiff, threshold)
    elif threshold > 0:
        return "not significant (%+.1f%%, |diff|=%.1f <= 3sigma=%.1f)" % (pct, absdiff, threshold)
    else:
        return "N/A (zero variance)"


def fmt_num(val):
    # Format a numeric stat value for display, "N/A" passes through.
    if val in ("N/A", None):
        return "N/A"
    return "%.3f" % float(val)


def format_values(median_val, vals):
    # Format list of values as "median [v1, v2, ...]"
    median_str = fmt_num(median_val)
    if len(vals) <= 1:
        return median_str
    joined = ", ".join(fmt_num(v) for v in vals)
    return "%s [%s]" % (median_str, joined)


# --- ydbd acquisition ---
def acquire_main_ydbd(args, results_dir):
    if args.main_ydbd:
        main_ydbd = str(Path(args.main_ydbd).resolve())
        print(f"Using pre-built {args.s3_ref} ydbd: {main_ydbd}")
        return main_ydbd

    main_ydbd = str(results_dir / f"ydbd-{args.s3_ref}-{args.build_preset}")
    if os.path.isfile(main_ydbd) and os.access(main_ydbd, os.X_OK):
        print(f"Using cached {args.s3_ref} ydbd: {main_ydbd}")
        return main_ydbd

    print(f"=== Downloading ydbd from S3 ({args.s3_ref}/{args.build_preset}) ===")
    s3_url = f"{S3_BASE_URL}/{args.s3_ref}/{args.build_preset}/ydbd"
    print(f"URL: {s3_url}")
    urllib.request.urlretrieve(s3_url, main_ydbd)
    os.chmod(main_ydbd, 0o755)
    print(f"Downloaded: {main_ydbd}")
    return main_ydbd


def acquire_current_ydbd(args, current_branch):
    if args.current_ydbd:
        current_ydbd = str(Path(args.current_ydbd).resolve())
        print(f"Using pre-built current ydbd: {current_ydbd}")
        return current_ydbd

    print(f"=== Building ydbd from current branch ({current_branch}, preset: {args.build_preset}) ===")
    proc = subprocess.run(
        ["./ya", "make", "--build", args.build_preset, "ydb/apps/ydbd", "ydb/apps/ydb"],
        cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    print("\n".join(proc.stdout.splitlines()[-5:]))
    if proc.returncode != 0:
        # Match the bash original (set -euo pipefail): abort instead of silently
        # benchmarking a stale ydbd left over from a previous build.
        print(f"ERROR: build failed (exit code {proc.returncode})")
        sys.exit(1)
    current_ydbd = str(REPO_ROOT / "ydb/apps/ydbd/ydbd")
    print(f"Current ydbd built: {current_ydbd}")
    return current_ydbd


# --- Test execution & parsing ---
def run_test(args, label, ydbd_path, test_path, log_file, extra_params):
    print(f"--- Running {test_path} with {label} ydbd ---")
    shutil.rmtree(REPO_ROOT / test_path / "test-results", ignore_errors=True)
    cmd = [
        "./ya", "make", "--build", args.build_preset, "-tA", test_path,
        f"-DYDB_DRIVER_BINARY_PREBUILT={ydbd_path}",
        "--test-param", f"stress_default_duration={args.duration}",
    ] + extra_params
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    with open(log_file, "w") as f:
        f.write(proc.stdout)
    print("\n".join(proc.stdout.splitlines()[-5:]))
    print("")


def extract_total_txs_sec(log_dir):
    test_log = None
    for path in sorted(Path(log_dir).rglob("*.log")):
        if "testing_out_stuff" in path.parts:
            test_log = path
            break
    if test_log is None:
        return None

    # Mirror the bash original `grep -A1 "^Total" | tail -1 | awk '{print $3}'`:
    # take the line after the LAST "Total" header, 3rd whitespace field.
    lines = test_log.read_text(errors="replace").splitlines()
    val = None
    for i, line in enumerate(lines):
        if line.startswith("Total") and i + 1 < len(lines):
            fields = lines[i + 1].split()
            if len(fields) >= 3:
                val = fields[2]
    return val


def join_param(items):
    # Comma-separated, like `IFS=,; echo "${arr[*]}"` in the shell version.
    return ",".join(items)


def collect_value(values, val):
    # Append a parsed Txs/Sec value, skipping missing/empty/non-numeric tokens
    # (matches the bash `[[ "$val" != "N/A" && -n "$val" ]]` guard).
    if val is None or val == "" or val == "N/A":
        return
    try:
        values.append(float(val))
    except ValueError:
        pass


# --- Workload runners ---
def run_vector_workload(args, results_dir, main_ydbd, current_ydbd):
    print("")
    print("==========================================")
    print(f"  Vector workload ({args.iterations} iterations, interleaved)")
    print("==========================================")

    vector_test = "ydb/tests/stress/vector_workload/tests"
    vector_data_dir = str(results_dir / "vector_data")
    log_dir = REPO_ROOT / vector_test / "test-results/py3test/testing_out_stuff"

    main_flags = join_param(args.main_feature_flag)
    current_flags = join_param(args.current_feature_flag)
    main_tsc = join_param(args.main_table_service_config)
    current_tsc = join_param(args.current_table_service_config)

    main_values = []
    current_values = []

    # First iteration: generate mode creates the query table.
    # Subsequent iterations: load mode reuses the query table.
    for i in range(1, args.iterations + 1):
        print("")
        print(f"=== Vector iteration {i}/{args.iterations} ===")

        # Run main
        local_mode = "generate" if i == 1 else "load"
        extra = [
            "--test-param", f"vector_mode={local_mode}",
            "--test-param", f"vector_data_dir={vector_data_dir}",
            "--test-param", f"vector_targets={args.targets}",
            "--test-param", f"vector_warmup={args.warmup}",
            "--test-param", f"vector_rows={args.rows}",
            "--test-param", f"vector_threads={args.threads}",
        ]
        if main_flags:
            extra += ["--test-param", f"feature_flags={main_flags}"]
        if main_tsc:
            extra += ["--test-param", f"table_service_config={main_tsc}"]
        run_test(args, args.s3_ref, main_ydbd, vector_test,
                 results_dir / f"vector_main_{i}.log", extra)
        val = extract_total_txs_sec(log_dir)
        collect_value(main_values, val)
        print(f"  {args.s3_ref} iteration {i}: {val} Txs/Sec")

        # Run current
        extra = [
            "--test-param", "vector_mode=load",
            "--test-param", f"vector_data_dir={vector_data_dir}",
            "--test-param", f"vector_targets={args.targets}",
            "--test-param", f"vector_warmup={args.warmup}",
            "--test-param", f"vector_rows={args.rows}",
            "--test-param", f"vector_threads={args.threads}",
        ]
        if current_flags:
            extra += ["--test-param", f"feature_flags={current_flags}"]
        if current_tsc:
            extra += ["--test-param", f"table_service_config={current_tsc}"]
        run_test(args, "current", current_ydbd, vector_test,
                 results_dir / f"vector_current_{i}.log", extra)
        val = extract_total_txs_sec(log_dir)
        collect_value(current_values, val)
        print(f"  current iteration {i}: {val} Txs/Sec")

    return build_result(main_values, current_values)


def run_fulltext_workload(args, results_dir, main_ydbd, current_ydbd):
    print("")
    print("==========================================")
    print(f"  Fulltext workload ({args.iterations} iterations, interleaved)")
    print("==========================================")

    fulltext_test = "ydb/tests/stress/fulltext_workload/tests"
    log_dir = REPO_ROOT / fulltext_test / "test-results/py3test/testing_out_stuff"

    main_flags = join_param(args.main_feature_flag)
    current_flags = join_param(args.current_feature_flag)
    main_tsc = join_param(args.main_table_service_config)
    current_tsc = join_param(args.current_table_service_config)

    main_values = []
    current_values = []

    for i in range(1, args.iterations + 1):
        print("")
        print(f"=== Fulltext iteration {i}/{args.iterations} ===")

        base_extra = [
            "--test-param", f"fulltext_rows={args.rows}",
            "--test-param", f"fulltext_threads={args.threads}",
            "--test-param", f"fulltext_targets={args.targets}",
        ]

        # Run main
        extra = list(base_extra)
        if main_flags:
            extra += ["--test-param", f"feature_flags={main_flags}"]
        if main_tsc:
            extra += ["--test-param", f"table_service_config={main_tsc}"]
        run_test(args, args.s3_ref, main_ydbd, fulltext_test,
                 results_dir / f"fulltext_main_{i}.log", extra)
        val = extract_total_txs_sec(log_dir)
        collect_value(main_values, val)
        print(f"  {args.s3_ref} iteration {i}: {val} Txs/Sec")

        # Run current
        extra = list(base_extra)
        if current_flags:
            extra += ["--test-param", f"feature_flags={current_flags}"]
        if current_tsc:
            extra += ["--test-param", f"table_service_config={current_tsc}"]
        run_test(args, "current", current_ydbd, fulltext_test,
                 results_dir / f"fulltext_current_{i}.log", extra)
        val = extract_total_txs_sec(log_dir)
        collect_value(current_values, val)
        print(f"  current iteration {i}: {val} Txs/Sec")

    return build_result(main_values, current_values)


def build_result(main_values, current_values):
    res = {
        "main_values": main_values,
        "current_values": current_values,
        "main_txs": median(main_values),
        "current_txs": median(current_values),
        "main_mean": mean(main_values),
        "current_mean": mean(current_values),
        "main_stddev": stddev(main_values),
        "current_stddev": stddev(current_values),
    }
    res["main_detail"] = format_values(res["main_txs"], main_values)
    res["current_detail"] = format_values(res["current_txs"], current_values)
    res["significance"] = calc_significance(
        res["main_mean"], res["current_mean"],
        res["main_stddev"], res["current_stddev"],
        len(main_values), len(current_values),
    )
    return res


# --- Reporting ---
def print_report(args, current_branch, vector, fulltext):
    vector_diff = calc_diff(vector["main_txs"], vector["current_txs"]) if vector else "N/A"
    fulltext_diff = calc_diff(fulltext["main_txs"], fulltext["current_txs"]) if fulltext else "N/A"

    print("")
    print("==========================================")
    print("  Performance Comparison Results")
    print(f"  Build preset: {args.build_preset}")
    print(f"  Iterations: {args.iterations} (median reported)")
    print("==========================================")
    print("")
    print("%-20s %15s %15s %10s" % ("Workload", f"{args.s3_ref} (Txs/Sec)",
                                     f"{current_branch} (Txs/Sec)", "Diff"))
    print("%-20s %15s %15s %10s" % ("-" * 20, "-" * 15, "-" * 15, "-" * 10))
    if vector:
        print("%-20s %15s %15s %10s" % ("vector select", fmt_num(vector["main_txs"]),
                                        fmt_num(vector["current_txs"]), vector_diff))
    if fulltext:
        print("%-20s %15s %15s %10s" % ("fulltext select", fmt_num(fulltext["main_txs"]),
                                        fmt_num(fulltext["current_txs"]), fulltext_diff))
    print("")
    if vector:
        print("Vector details:")
        print(f"  {args.s3_ref}:   {vector['main_detail']}  "
              f"(mean={fmt_num(vector['main_mean'])}, σ={fmt_num(vector['main_stddev'])})")
        print(f"  current: {vector['current_detail']}  "
              f"(mean={fmt_num(vector['current_mean'])}, σ={fmt_num(vector['current_stddev'])})")
        print(f"  3σ test: {vector['significance']}")
    if fulltext:
        print("Fulltext details:")
        print(f"  {args.s3_ref}:   {fulltext['main_detail']}  "
              f"(mean={fmt_num(fulltext['main_mean'])}, σ={fmt_num(fulltext['main_stddev'])})")
        print(f"  current: {fulltext['current_detail']}  "
              f"(mean={fmt_num(fulltext['current_mean'])}, σ={fmt_num(fulltext['current_stddev'])})")
        print(f"  3σ test: {fulltext['significance']}")


def write_markdown_report(args, current_branch, results_dir, vector, fulltext):
    vector_diff = calc_diff(vector["main_txs"], vector["current_txs"]) if vector else "N/A"
    fulltext_diff = calc_diff(fulltext["main_txs"], fulltext["current_txs"]) if fulltext else "N/A"

    report_file = results_dir / "report.md"
    lines = []
    lines.append(f"## Performance Comparison: `{args.s3_ref}` vs `{current_branch}`")
    lines.append("")
    lines.append(f"**Build preset:** `{args.build_preset}` | **Duration:** {args.duration}s "
                 f"per workload | **Iterations:** {args.iterations} (median reported)")
    lines.append("")
    lines.append(f"| Workload | {args.s3_ref} (Txs/Sec) | {current_branch} (Txs/Sec) | Diff | 3σ significance |")
    lines.append("|---|---|---|---|---|")
    if vector:
        lines.append(f"| vector select | {vector['main_detail']} | {vector['current_detail']} "
                     f"| {vector_diff} | {vector['significance']} |")
    if fulltext:
        lines.append(f"| fulltext select | {fulltext['main_detail']} | {fulltext['current_detail']} "
                     f"| {fulltext_diff} | {fulltext['significance']} |")
    report_file.write_text("\n".join(lines) + "\n")
    print("")
    print(f"Markdown report: {report_file}")


def main():
    args = parse_args()
    results_dir = REPO_ROOT / "benchmark_results"
    results_dir.mkdir(parents=True, exist_ok=True)

    current_branch = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "--abbrev-ref", "HEAD"],
        stdout=subprocess.PIPE, text=True, check=True,
    ).stdout.strip()

    print(f"=== Performance comparison: {args.s3_ref} vs {current_branch} ===")
    print(f"Build preset: {args.build_preset}")
    print(f"Duration per run: {args.duration}s")
    print(f"Workload: {args.workload}")
    print(f"Vector targets: {args.targets}")
    print(f"Iterations: {args.iterations}")
    print(f"Warmup: {args.warmup}s")
    print(f"Rows: {args.rows}")
    print(f"Threads: {args.threads}")
    if args.main_feature_flag:
        print(f"Main feature flags: {' '.join(args.main_feature_flag)}")
    if args.current_feature_flag:
        print(f"Current feature flags: {' '.join(args.current_feature_flag)}")
    if args.main_table_service_config:
        print(f"Main table_service_config: {' '.join(args.main_table_service_config)}")
    if args.current_table_service_config:
        print(f"Current table_service_config: {' '.join(args.current_table_service_config)}")
    print("")

    main_ydbd = acquire_main_ydbd(args, results_dir)
    current_ydbd = acquire_current_ydbd(args, current_branch)

    for binary in (main_ydbd, current_ydbd):
        if not (os.path.isfile(binary) and os.access(binary, os.X_OK)):
            print(f"ERROR: Binary not found or not executable: {binary}")
            sys.exit(1)

    vector = None
    fulltext = None
    if args.workload in ("all", "vector"):
        vector = run_vector_workload(args, results_dir, main_ydbd, current_ydbd)
    if args.workload in ("all", "fulltext"):
        fulltext = run_fulltext_workload(args, results_dir, main_ydbd, current_ydbd)

    print_report(args, current_branch, vector, fulltext)
    print("")
    print(f"Detailed logs: {results_dir}/")
    write_markdown_report(args, current_branch, results_dir, vector, fulltext)


if __name__ == "__main__":
    main()
