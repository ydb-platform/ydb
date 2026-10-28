#!/usr/bin/env bash
# Interleaved A/B benchmark of block and scalar hash join.
#
# Usage: bash run_perf_ab.sh [options] NAME=SPEC [NAME=SPEC ...]
# SPEC is an executable join_perf binary or a git ref. The first NAME is the
# baseline; every speedup is baseline time / variant time.
#
# By default, run the 9 original PR cases and all cases from analytic_suite.py,
# deduplicated (currently 30), for block-hash and scalar-hash. Case definitions,
# block sizes and presets come from the checkout containing this script.
# Requires Python 3, Bash and a checkout containing the merged analytic suite.
# Git refs additionally require git and the repository's ya build tool.
#
# Options:
#   --repo DIR        checkout used to build refs (default: current directory)
#   --rounds N        interleaved rounds (default: 1)
#   --samples N       timed samples per invocation (default: 3; plus 1 warmup)
#   --algos LIST      comma list (default: block-hash,scalar-hash)
#   --cases LIST      comma list, "default" or "all" (default: default)
#                     "all" also includes the legacy h_lineitem_orders_f case
#   --cpu N           pin runs with taskset (default: no pinning)
#   --perf-stat       additionally collect hardware counters when available
#   --out DIR         work and result directory (default: hash_join_perf_out)
#   --ya-flags "..."  extra flags for relwithdebinfo builds
#   --list            list available case names and exit
#
# Examples (prebuilt binaries or fetched refs):
#   bash run_perf_ab.sh main=join_perf_main pr=join_perf_pr
#   bash run_perf_ab.sh --cpu 3 main=upstream/main pr=origin/hash-join/batched-lookup-main
#
# Output: Markdown table, results.json and per-invocation runs.jsonl.
# Returns 1 if a benchmark fails or output row counts differ.
# Ref builds use separate git worktrees and relwithdebinfo, without changing
# the source checkout. Each benchmark invocation runs sequentially.
set -euo pipefail
exec python3 - "$0" "$@" <<'PY'
import sys, os, json, subprocess, statistics, shutil, tempfile, argparse, csv, io, datetime, runpy, re

here = os.path.dirname(os.path.realpath(sys.argv.pop(1)))
TARGET_DIR = "ydb/library/yql/dq/comp_nodes/ut/join_perf"
# Historical cases not defined by the analytic suite.
PR_CASES = [
    ('h_lineitem_orders_w', 'TpchLineitemOrdersFiltered', 'inner', 'int', 'wide', '1:1', 'none'),
    ('ds_fact_item', 'TpcdsFactItem', 'inner', 'int32', 'tpc', '1:1', 'none'),
    ('h_lineitem_supplier', 'TpchLineitemSupplier', 'inner', 'int', 'tpc', '1:1', 'none'),
    ('h_lineitem_orders_f', 'TpchLineitemOrdersFiltered', 'inner', 'int', 'tpc', '0.25:1', 'none'),
    ('h_orders_semi', 'TpchOrdersLineitem', 'left-semi', 'int', 'tpc', '0.9:4', 'none'),
    ('ds_fact_customer_n', 'TpcdsFactCustomer', 'inner', 'nint32', 'tpc', '1:1', 'none'),
]
ORIGINAL_DEFAULT = [
    'fact_fact_inner',
    'h_lineitem_orders_w',
    'ds_fact_item',
    'dim1k_inner',
    'dim512k_inner_wide',
    'sel_01',
    'h_lineitem_supplier',
    'h_orders_semi',
    'ds_fact_customer_n',
]

def positive_int(value):
    n = int(value)
    if n <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return n

ap = argparse.ArgumentParser(prog="run_perf_ab.sh", add_help=True)
ap.add_argument("--repo", default=os.getcwd())
ap.add_argument("--rounds", type=positive_int, default=1)
ap.add_argument("--samples", type=positive_int, default=3)
ap.add_argument("--algos", default="block-hash,scalar-hash")
ap.add_argument("--cases", default="default")
ap.add_argument("--cpu", default=None)
ap.add_argument("--perf-stat", action="store_true")
ap.add_argument("--out", default="hash_join_perf_out")
ap.add_argument("--ya-flags", default="")
ap.add_argument("--list", action="store_true")
ap.add_argument("specs", nargs="*")
a = ap.parse_args()
suite_dir = here
if not os.path.isfile(os.path.join(suite_dir, "analytic_suite.py")):
    suite_dir = os.path.join(os.path.abspath(a.repo), TARGET_DIR)
if not os.path.isfile(os.path.join(suite_dir, "analytic_suite.py")):
    ap.error("analytic_suite.py is missing; use a checkout containing the merged analytic suite")
suite = runpy.run_path(os.path.join(suite_dir, "analytic_suite.py"))
BLOCKS = suite["BLOCKS"]
byname = {c[0]: tuple(c[:7]) for c in suite["CASES"]}
for case in PR_CASES:
    byname.setdefault(case[0], case)
DEFAULT = ORIGINAL_DEFAULT + [c[0] for c in suite["CASES"] if c[0] not in ORIGINAL_DEFAULT]
names = DEFAULT + [name for name in byname if name not in DEFAULT]
with open(os.path.join(suite_dir, "presets.json")) as source:
    PRESETS = json.load(source)
if a.list:
    print("\n".join(names)); sys.exit(0)
if len(a.specs) < 2:
    ap.error("need at least two NAME=SPEC arguments")
cases = names if a.cases == "all" else DEFAULT if a.cases == "default" else a.cases.split(",")
for c in cases:
    if c not in names:
        sys.exit("unknown case %s, see --list" % c)
algos = a.algos.split(",")
if any(algo not in ("block-hash", "scalar-hash") for algo in algos):
    ap.error("--algos must contain block-hash and/or scalar-hash")
if len(set(cases)) != len(cases) or len(set(algos)) != len(algos):
    ap.error("duplicate cases or algorithms")
for spec in a.specs:
    name, separator, val = spec.partition("=")
    if not separator or not val or not re.fullmatch(r"[A-Za-z0-9_-]+", name):
        ap.error("each binary must be NAME=SPEC; NAME may contain letters, digits, underscores and hyphens")
if len({spec.split("=", 1)[0] for spec in a.specs}) != len(a.specs):
    ap.error("binary names must be unique")
missing = [byname[c][1] for c in cases if byname[c][1] not in PRESETS]
if missing:
    ap.error("presets.json is missing presets: " + ", ".join(sorted(set(missing))))
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
    target = TARGET_DIR + "/bin"
    cmd = ["./ya", "make", "--build", "relwithdebinfo"] + a.ya_flags.split() + [target]
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
        raise RuntimeError("benchmark failed (exit %d):\n%s" % (p.returncode, p.stderr[-1500:]))
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

# Start a fresh raw log when reusing an output directory.
open(os.path.join(out, "runs.jsonl"), "w").close()
times, rows, pstat, fails = {}, {}, {}, []
for r in range(a.rounds):
    order = bins[r % len(bins):] + bins[:r % len(bins)]
    for algo in algos:
        for case_index, case in enumerate(cases):
            offset = case_index % len(order)
            case_order = order[offset:] + order[:offset]
            for name, b in case_order:
                try:
                    j = run(command(b, algo, case, a.samples))
                except RuntimeError as e:
                    fails.append(str(e)); print(e, file=sys.stderr); continue
                k = (algo, case, name)
                times.setdefault(k, []).append(j["medianCpuUs"] / 1e3)
                rows.setdefault(k, set()).add(j["outputRows"])
                with open(os.path.join(out, "runs.jsonl"), "a") as raw:
                    raw.write(json.dumps({"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "round": r + 1, "algo": algo, "case": case, "binary": name, "result": j}) + "\n")
                print("round %d/%d %s %s %s: %.3f ms, outputRows=%d" % (r + 1, a.rounds, algo, case, name, j["medianCpuUs"] / 1e3, j["outputRows"]), file=sys.stderr, flush=True)
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
