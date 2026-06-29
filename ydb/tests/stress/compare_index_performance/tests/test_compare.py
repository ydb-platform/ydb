# -*- coding: utf-8 -*-
#
# Compare vector/fulltext workload performance between a baseline ydbd and the
# current (locally built) ydbd, in a single py3test.
#
# The current ydbd is resolved as: an explicit path (compare_current_ydbd), else
# an S3 ref (compare_current_ref), else the locally built ydbd from the build
# harness. The baseline ydbd is resolved as: an explicit path
# (compare_baseline_ydbd), else an S3 download by compare_ref. Both S3 downloads
# use compare_build_preset. Setting compare_ref and compare_current_ref (or
# explicit paths) lets you compare two arbitrary prebuilt binaries (e.g. main vs
# main, or two different refs) without a local build.
#
# For each iteration the test starts an in-process KiKiMR cluster on each
# binary, runs the corresponding workload binary against it, parses the
# `Total ... Txs/Sec` line, then computes median/mean/sample-stddev and a 3-sigma
# significance test. A markdown report is written into testing_out_stuff.
#
# All knobs are supplied via `ya make --test-param compare_*=...`.
#
# IMPORTANT: the test's own build provides a single build preset for the current
# binary; compare_build_preset only selects which prebuilt baseline is fetched
# from S3 (and labels the report). Both binaries should use the same preset for
# a fair comparison.

import os
import statistics
import urllib.request

import pytest
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

S3_BASE_URL = "https://storage.yandexcloud.net/ydb-builds"


# --- Parameter parsing helpers ---
def _parse_feature_flags(raw):
    return [f for f in raw.split(',') if f]


def _parse_table_service_config(raw):
    tsc = {}
    for item in raw.split(','):
        if '=' in item:
            k, v = item.split('=', 1)
            if v.lower() == 'true':
                tsc[k] = True
            elif v.lower() == 'false':
                tsc[k] = False
            else:
                tsc[k] = v
    return tsc


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


def collect_value(values, val):
    # Append a parsed Txs/Sec value, skipping missing/empty/non-numeric tokens.
    if val is None or val == "" or val == "N/A":
        return
    try:
        values.append(float(val))
    except ValueError:
        pass


def extract_total_txs_sec(log_file):
    # Mirror the original `grep -A1 "^Total" | tail -1 | awk '{print $3}'`:
    # take the line after the LAST "Total" header, 3rd whitespace field.
    if not os.path.isfile(log_file):
        return None
    with open(log_file, errors="replace") as f:
        lines = f.read().splitlines()
    val = None
    for i, line in enumerate(lines):
        if line.startswith("Total") and i + 1 < len(lines):
            fields = lines[i + 1].split()
            if len(fields) >= 3:
                val = fields[2]
    return val


class TestCompareIndexPerformance:
    @pytest.fixture(autouse=True)
    def setup(self):
        # Binaries. Each side can be overridden with an explicit path; otherwise
        # the baseline is downloaded from S3 (compare_ref/compare_build_preset)
        # and the current binary comes from the local build harness.
        self.baseline_ydbd_param = yatest.common.get_param('compare_baseline_ydbd', default='')
        self.current_ydbd_param = yatest.common.get_param('compare_current_ydbd', default='')
        self.ref = yatest.common.get_param('compare_ref', default='main')
        self.current_ref = yatest.common.get_param('compare_current_ref', default='')
        self.build_preset = yatest.common.get_param('compare_build_preset', default='relwithdebinfo')

        # Workload knobs (kept as strings: the workload CLIs accept them as-is)
        self.iterations = int(yatest.common.get_param('compare_iterations', default='3'))
        self.duration = yatest.common.get_param('compare_duration', default='60')
        self.warmup = yatest.common.get_param('compare_warmup', default='30')
        self.rows = yatest.common.get_param('compare_rows', default='10000')
        self.threads = yatest.common.get_param('compare_threads', default='10')
        self.targets = yatest.common.get_param('compare_targets', default='1000')

        # Per-cluster config
        self.baseline_flags = _parse_feature_flags(
            yatest.common.get_param('compare_baseline_feature_flags', default=''))
        self.current_flags = _parse_feature_flags(
            yatest.common.get_param('compare_current_feature_flags', default=''))
        self.baseline_tsc = _parse_table_service_config(
            yatest.common.get_param('compare_baseline_table_service_config', default=''))
        self.current_tsc = _parse_table_service_config(
            yatest.common.get_param('compare_current_table_service_config', default=''))

        self.workload = yatest.common.get_param('compare_workload', default='all')

    # --- binary resolution ---
    def _download_ydbd(self, ref):
        # Download (and cache) a prebuilt ydbd for the given S3 ref + preset.
        dst = yatest.common.output_path(f"ydbd-{ref}-{self.build_preset}")
        if not os.path.isfile(dst):
            s3_url = f"{S3_BASE_URL}/{ref}/{self.build_preset}/ydbd"
            print(f"Downloading ydbd from {s3_url}")
            urllib.request.urlretrieve(s3_url, dst)
            os.chmod(dst, 0o755)
        return dst

    def _baseline_ydbd(self):
        # Explicit path wins; otherwise download from S3 (compare_ref/preset).
        if self.baseline_ydbd_param:
            return self.baseline_ydbd_param
        return self._download_ydbd(self.ref)

    def _current_ydbd(self):
        # Explicit path wins (e.g. comparing two prebuilt binaries); then an S3
        # ref (compare_current_ref); otherwise the locally built ydbd.
        if self.current_ydbd_param:
            return self.current_ydbd_param
        if self.current_ref:
            return self._download_ydbd(self.current_ref)
        return kikimr_driver_path()

    def _current_label(self):
        # Label the current side by its S3 ref when downloaded, else "current".
        return f"current({self.current_ref})" if self.current_ref else "current"

    # --- cluster lifecycle + single workload run ---
    def _run_one(self, label, ydbd_path, flags, tsc, run_workload, log_name):
        config = KikimrConfigGenerator(
            binary_paths=[ydbd_path],
            erasure=Erasure.from_string(yatest.common.get_param('stress_default_erasure', default='NONE')),
            extra_feature_flags=flags,
            table_service_config=tsc or None,
        )
        cluster = KiKiMR(config)
        cluster.start()
        try:
            endpoint = "grpc://localhost:%s" % cluster.nodes[1].port
            log_file = yatest.common.output_path(log_name)
            err_file = yatest.common.output_path(log_name + ".err")
            print(f"--- Running {label} workload against {ydbd_path} ---")
            # stdout and stderr go to separate files: the Txs/Sec line is parsed
            # from stdout, so stderr noise must not interleave into it.
            with open(log_file, "w") as out, open(err_file, "w") as err:
                run_workload(endpoint, out, err)
            return extract_total_txs_sec(log_file)
        finally:
            cluster.stop()

    def _exec_workload(self, binary_env, endpoint, out, err, extra):
        cmd = [
            yatest.common.binary_path(os.environ[binary_env]),
            "--endpoint", endpoint,
            "--database", "/Root",
            "--duration", self.duration,
        ] + extra
        yatest.common.execute(cmd, stdout=out, stderr=err)

    # --- statistics + report ---
    def _summarize(self, main_values, current_values):
        res = {
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
        res["diff"] = calc_diff(res["main_txs"], res["current_txs"])
        return res

    def _report(self, slug, workload_name, res):
        current_label = self._current_label()
        print("")
        print("==========================================")
        print(f"  {workload_name} comparison: {self.ref} vs {current_label}")
        print(f"  Build preset: {self.build_preset} | Iterations: {self.iterations} (median reported)")
        print("==========================================")
        print("%-20s %18s %18s %10s" % ("Workload", f"{self.ref} (Txs/Sec)",
                                        f"{current_label} (Txs/Sec)", "Diff"))
        print("%-20s %18s %18s %10s" % ("-" * 20, "-" * 18, "-" * 18, "-" * 10))
        print("%-20s %18s %18s %10s" % (workload_name, fmt_num(res["main_txs"]),
                                        fmt_num(res["current_txs"]), res["diff"]))
        print(f"  {self.ref}:   {res['main_detail']}  "
              f"(mean={fmt_num(res['main_mean'])}, σ={fmt_num(res['main_stddev'])})")
        print(f"  {current_label}: {res['current_detail']}  "
              f"(mean={fmt_num(res['current_mean'])}, σ={fmt_num(res['current_stddev'])})")
        print(f"  3σ test: {res['significance']}")

        # One report file per workload method: yatest output_path is shared
        # across test methods, so a fixed name would let the second method
        # overwrite the first.
        report_file = yatest.common.output_path(f"report_{slug}.md")
        with open(report_file, "w") as f:
            f.write(f"## Performance Comparison: `{self.ref}` vs `{current_label}` ({workload_name})\n\n")
            f.write(f"**Build preset:** `{self.build_preset}` | **Duration:** {self.duration}s "
                    f"per workload | **Iterations:** {self.iterations} (median reported)\n\n")
            f.write(f"| Workload | {self.ref} (Txs/Sec) | {current_label} (Txs/Sec) | Diff | 3σ significance |\n")
            f.write("|---|---|---|---|---|\n")
            f.write(f"| {workload_name} | {res['main_detail']} | {res['current_detail']} "
                    f"| {res['diff']} | {res['significance']} |\n")
        print(f"Markdown report: {report_file}")

    # --- tests ---
    def test_vector(self):
        if self.workload not in ("all", "vector"):
            pytest.skip(f"compare_workload={self.workload} excludes vector")

        baseline_ydbd = self._baseline_ydbd()
        current_ydbd = self._current_ydbd()
        data_dir = yatest.common.output_path("vector_data")

        main_values = []
        current_values = []
        # First iteration: baseline runs in generate mode (creates + dumps the
        # query table). Subsequent baseline runs and all current runs use load
        # mode, reusing the dumped query table.
        for i in range(1, self.iterations + 1):
            print(f"=== Vector iteration {i}/{self.iterations} ===")

            baseline_mode = "generate" if i == 1 else "load"

            def baseline_workload(endpoint, out, err, mode=baseline_mode):
                self._exec_workload("YDB_VECTOR_WORKLOAD_PATH", endpoint, out, err, [
                    "--mode", mode, "--data-dir", data_dir,
                    "--targets", self.targets, "--warmup", self.warmup,
                    "--rows", self.rows, "--threads", self.threads,
                ])

            def current_workload(endpoint, out, err):
                self._exec_workload("YDB_VECTOR_WORKLOAD_PATH", endpoint, out, err, [
                    "--mode", "load", "--data-dir", data_dir,
                    "--targets", self.targets, "--warmup", self.warmup,
                    "--rows", self.rows, "--threads", self.threads,
                ])

            collect_value(main_values, self._run_one(
                self.ref, baseline_ydbd, self.baseline_flags, self.baseline_tsc,
                baseline_workload, f"vector_main_{i}.log"))
            collect_value(current_values, self._run_one(
                "current", current_ydbd, self.current_flags, self.current_tsc,
                current_workload, f"vector_current_{i}.log"))

        self._report("vector", "vector select", self._summarize(main_values, current_values))

    def test_fulltext(self):
        if self.workload not in ("all", "fulltext"):
            pytest.skip(f"compare_workload={self.workload} excludes fulltext")

        baseline_ydbd = self._baseline_ydbd()
        current_ydbd = self._current_ydbd()

        # Fulltext requires the feature flag enabled on both clusters.
        baseline_flags = self.baseline_flags + ["enable_fulltext_index"]
        current_flags = self.current_flags + ["enable_fulltext_index"]

        main_values = []
        current_values = []
        for i in range(1, self.iterations + 1):
            print(f"=== Fulltext iteration {i}/{self.iterations} ===")

            def fulltext_workload(endpoint, out, err):
                self._exec_workload("YDB_FULLTEXT_WORKLOAD_PATH", endpoint, out, err, [
                    "--rows", self.rows, "--targets", self.targets,
                    "--threads", self.threads,
                ])

            collect_value(main_values, self._run_one(
                self.ref, baseline_ydbd, baseline_flags, self.baseline_tsc,
                fulltext_workload, f"fulltext_main_{i}.log"))
            collect_value(current_values, self._run_one(
                "current", current_ydbd, current_flags, self.current_tsc,
                fulltext_workload, f"fulltext_current_{i}.log"))

        self._report("fulltext", "fulltext select", self._summarize(main_values, current_values))
