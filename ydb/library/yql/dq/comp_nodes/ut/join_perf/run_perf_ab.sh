#!/usr/bin/env bash
# Interleaved A/B benchmark of the YDB hash join (join_perf), block-hash and scalar-hash.
#
# Usage:
#   run_perf_ab.sh [options] NAME=SPEC [NAME=SPEC ...]
#     SPEC is either a path to a prebuilt join_perf binary or a git ref (branch, tag, sha).
#     The first NAME is the baseline; speedups are reported relative to it.
#
# Options:
#   --repo DIR        ydb checkout used to build refs (default: current directory)
#   --rounds N        interleaved rounds (default 5)
#   --samples N       samples per join_perf run, median is taken (default 9)
#   --algos LIST      comma list, default block-hash,scalar-hash
#   --cases LIST      comma list of case names, or "all" (default: the 4 main cases + a few more)
#   --cpu N           pin every run to CPU N with taskset (default: no pinning)
#   --perf-stat       also run each case once per round under `perf stat` and report cycles,
#                     instructions and cache-misses (needs hardware counters; skipped if unavailable)
#   --out DIR         work and result directory (default ./hash_join_perf_out)
#   --ya-flags "..."  extra flags for ya make when building refs (default: none)
#   --list            print case names and exit
#
# Examples:
#   ./run_perf_ab.sh --repo ~/ydb base=origin/main new=hash-join/batched-lookup
#   ./run_perf_ab.sh --rounds 7 --cpu 3 --perf-stat base=/tmp/jp_main new=/tmp/jp_new
#
# Output: markdown table of median CPU ms per algo/case/binary (median over rounds of the per-run
# median), speedups vs the first binary, and a check that outputRows is identical for all binaries.
# Exit code is 1 if outputRows differ or a run fails. Results are also saved to OUT/results.json.
# Refs are built in a git worktree OUT/wt_<name> with: ya make --build release <ya-flags>
#   ydb/library/yql/dq/comp_nodes/ut/join_perf/bin
set -euo pipefail
exec python3 - "$@" <<'PY'
import sys, os, json, subprocess, statistics, shutil, tempfile, argparse, csv, io

BLOCKS = "8192,6000,8192,2048"
PRESETS = {"AnDim1K": [[1048576, 1024]], "AnDim32K": [[1048576, 32768]], "AnDim512K": [[1048576, 524288]],
           "AnFactFact": [[1048576, 1048576]], "TpchLineitemSupplier": [[1048576, 16384]],
           "TpchLineitemOrdersFiltered": [[1048576, 262144]], "TpchOrdersLineitem": [[262144, 1048576]],
           "TpcdsFactItem": [[1048576, 16384]], "TpcdsFactCustomer": [[1048576, 131072]]}
CASES = [
 ("fact_fact_inner",      "AnFactFact",                 "inner",     "int",    "narrow", "1:1",     "none"),
 ("h_lineitem_orders_w",  "TpchLineitemOrdersFiltered", "inner",     "int",    "wide",   "1:1",     "none"),
 ("ds_fact_item",         "TpcdsFactItem",              "inner",     "int32",  "tpc",    "1:1",     "none"),
 ("dim1k_inner",          "AnDim1K",                    "inner",     "int",    "narrow", "1:1",     "none"),
 ("dim32k_inner",         "AnDim32K",                   "inner",     "int",    "narrow", "1:1",     "none"),
 ("dim512k_inner",        "AnDim512K",                  "inner",     "int",    "narrow", "1:1",     "none"),
 ("dim512k_inner_wide",   "AnDim512K",                  "inner",     "int",    "wide",   "1:1",     "none"),
 ("sel_01",               "AnDim512K",                  "inner",     "int",    "narrow", "0.001:1", "none"),
 ("dups4_inner",          "AnDim32K",                   "inner",     "int",    "narrow", "1:4",     "none"),
 ("left_dim512k_wide",    "AnDim512K",                  "left",      "int",    "wide",   "0.9:1",   "none"),
 ("str_dim512k",          "AnDim512K",                  "inner",     "string", "narrow", "1:1",     "none"),
 ("h_lineitem_supplier",  "TpchLineitemSupplier",       "inner",     "int",    "tpc",    "1:1",     "none"),
 ("h_lineitem_orders_f",  "TpchLineitemOrdersFiltered", "inner",     "int",    "tpc",    "0.25:1",  "none"),
 ("h_orders_semi",        "TpchOrdersLineitem",         "left-semi", "int",    "tpc",    "0.9:4",   "none"),
 ("ds_fact_customer_n",   "TpcdsFactCustomer",          "inner",     "nint32", "tpc",    "1:1",     "none"),
]
DEFAULT = ["fact_fact_inner", "h_lineitem_orders_w", "ds_fact_item", "dim1k_inner", "dim512k_inner_wide",
           "sel_01", "h_lineitem_supplier", "h_orders_semi", "ds_fact_customer_n"]

ap = argparse.ArgumentParser(prog="run_perf_ab.sh", add_help=True)
ap.add_argument("--repo", default=os.getcwd())
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--samples", type=int, default=9)
ap.add_argument("--algos", default="block-hash,scalar-hash")
ap.add_argument("--cases", default=",".join(DEFAULT))
ap.add_argument("--cpu", default=None)
ap.add_argument("--perf-stat", action="store_true")
ap.add_argument("--out", default="hash_join_perf_out")
ap.add_argument("--ya-flags", default="")
ap.add_argument("--list", action="store_true")
ap.add_argument("specs", nargs="*")
a = ap.parse_args()
if a.list:
    print("\n".join(c[0] for c in CASES)); sys.exit(0)
if len(a.specs) < 2:
    ap.error("need at least two NAME=SPEC arguments")
names = [c[0] for c in CASES]
cases = names if a.cases == "all" else a.cases.split(",")
for c in cases:
    if c not in names:
        sys.exit("unknown case %s, see --list" % c)
byname = {c[0]: c for c in CASES}
algos = a.algos.split(",")
out = os.path.abspath(a.out); os.makedirs(out, exist_ok=True)
presets_dir = os.path.join(out, "presets"); os.makedirs(presets_dir, exist_ok=True)
json.dump(PRESETS, open(os.path.join(presets_dir, "presets.json"), "w"))

bins = []
for spec in a.specs:
    if "=" not in spec:
        sys.exit("bad spec %s, expected NAME=SPEC" % spec)
    name, val = spec.split("=", 1)
    if os.path.isfile(val) and os.access(val, os.X_OK):
        bins.append((name, os.path.abspath(val))); continue
    repo = os.path.abspath(a.repo)
    wt = os.path.join(out, "wt_" + name)
    if not os.path.isdir(wt):
        subprocess.check_call(["git", "-C", repo, "worktree", "add", "--detach", wt, val])
    else:
        subprocess.check_call(["git", "-C", wt, "checkout", "--detach", val])
    target = "ydb/library/yql/dq/comp_nodes/ut/join_perf/bin"
    cmd = ["./ya", "make", "--build", "release"] + a.ya_flags.split() + [target]
    print("building %s (%s): %s" % (name, val, " ".join(cmd)), file=sys.stderr)
    subprocess.check_call(cmd, cwd=wt)
    src = os.path.realpath(os.path.join(wt, target, "join_perf"))
    dst = os.path.join(out, "join_perf_" + name)
    shutil.copyfile(src, dst); os.chmod(dst, 0o755)
    bins.append((name, dst))

def command(b, algo, case, samples):
    _, preset, kind, ks, payload, sel, flt = byname[case]
    cmd = [b, "--presets-path", presets_dir, "--preset", preset, "--preset-flavour", "same-size",
           "--scale", "1", "--samples", str(samples), "--warmup", "1", "--algo", algo,
           "--key-schema", ks, "--payload", payload, "--selectivity", sel, "--join-kind", kind,
           "--filter", flt, "--block-size", BLOCKS]
    if a.cpu is not None:
        cmd = ["taskset", "-c", str(a.cpu)] + cmd
    return cmd

def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    lines = [l for l in p.stdout.splitlines() if l.startswith("{")]
    if p.returncode != 0 or not lines:
        raise RuntimeError("failed: %s\n%s" % (" ".join(cmd), p.stderr[-1500:]))
    return json.loads(lines[-1])

EVENTS = "cycles,instructions,cache-misses"
perf_ok = False
if a.perf_stat:
    t = subprocess.run(["perf", "stat", "-x,", "-e", EVENTS, "true"], capture_output=True, text=True)
    perf_ok = t.returncode == 0 and "<not supported>" not in t.stderr and "<not counted>" not in t.stderr
    if not perf_ok:
        print("perf stat counters are not available, skipping", file=sys.stderr)

def perf_run(cmd):
    p = subprocess.run(["perf", "stat", "-x,", "-e", EVENTS] + cmd, capture_output=True, text=True)
    res = {}
    for row in csv.reader(io.StringIO(p.stderr)):
        if len(row) > 2 and row[0].replace(".", "").isdigit():
            res[row[2].split(":")[0]] = float(row[0])
    return res

times, rows, pstat, fails = {}, {}, {}, []
for r in range(a.rounds):
    order = bins[r % len(bins):] + bins[:r % len(bins)]
    for algo in algos:
        for case in cases:
            for name, b in order:
                try:
                    j = run(command(b, algo, case, a.samples))
                except RuntimeError as e:
                    fails.append(str(e)); print(e, file=sys.stderr); continue
                k = (algo, case, name)
                times.setdefault(k, []).append(j["medianCpuUs"] / 1e3)
                rows.setdefault(k, set()).add(j["outputRows"])
                if perf_ok:
                    pstat.setdefault(k, []).append(perf_run(command(b, algo, case, 3)))
    print("round %d/%d done" % (r + 1, a.rounds), file=sys.stderr)

names_b = [n for n, _ in bins]
print("| algo | case | " + " | ".join(names_b) + " | speedup vs " + names_b[0] + " | outputRows |")
print("|" + "---|" * (len(names_b) + 4))
bad = bool(fails)
result = {}
for algo in algos:
    for case in cases:
        med = [statistics.median(times[(algo, case, n)]) if (algo, case, n) in times else float("nan") for n in names_b]
        rs = set().union(*[rows.get((algo, case, n), set()) for n in names_b])
        same = len(rs) == 1
        bad |= not same
        sp = " ".join("%.2fx" % (med[0] / m) for m in med[1:])
        print("| %s | %s | %s | %s | %s |" % (algo, case, " | ".join("%.1f" % m for m in med), sp,
              "same (%d)" % next(iter(rs)) if same else "DIFFER %s" % sorted(rs)))
        result["%s/%s" % (algo, case)] = {"ms": dict(zip(names_b, med)), "outputRows": sorted(rs)}
if perf_ok:
    print("\nperf stat (median over rounds, per run of 3 samples + warmup):\n")
    print("| algo | case | binary | cycles | instructions | IPC | cache-misses |")
    print("|---|---|---|---|---|---|---|")
    for algo in algos:
        for case in cases:
            for n in names_b:
                v = pstat.get((algo, case, n))
                if not v:
                    continue
                g = lambda e: statistics.median([x.get(e, float("nan")) for x in v])
                cy, ins, cm = g("cycles"), g("instructions"), g("cache-misses")
                print("| %s | %s | %s | %.3g | %.3g | %.2f | %.3g |" % (algo, case, n, cy, ins, ins / cy, cm))
json.dump(result, open(os.path.join(out, "results.json"), "w"), indent=1)
sys.exit(1 if bad else 0)
PY
