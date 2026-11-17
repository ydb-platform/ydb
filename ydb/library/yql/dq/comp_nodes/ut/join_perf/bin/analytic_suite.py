#!/usr/bin/env python3
import argparse, json, os, subprocess, sys, concurrent.futures as cf

BLOCKS = "8192,6000,8192,2048"

CASES = [
 ("dim1k_inner",        "AnDim1K",    "inner",     "int",     "narrow", "1:1",    "none",   "fact JOIN tiny dimension, every row matches"),
 ("dim32k_inner",       "AnDim32K",   "inner",     "int",     "narrow", "1:1",    "none",   "fact JOIN small dimension, every row matches"),
 ("dim512k_inner",      "AnDim512K",  "inner",     "int",     "narrow", "1:1",    "none",   "fact JOIN big dimension, every row matches"),
 ("dim32k_inner_wide",  "AnDim32K",   "inner",     "int",     "wide",   "1:1",    "none",   "fact JOIN small dimension, wide rows"),
 ("dim512k_inner_wide", "AnDim512K",  "inner",     "int",     "wide",   "1:1",    "none",   "fact JOIN big dimension, wide rows"),
 ("fact_fact_inner",    "AnFactFact", "inner",     "int",     "narrow", "1:1",    "none",   "two tables of the same size"),
 ("sel_50",             "AnDim32K",   "inner",     "int",     "narrow", "0.5:1",  "none",   "low selectivity probe: half of the rows match"),
 ("sel_5",              "AnDim32K",   "inner",     "int",     "narrow", "0.05:1", "none",   "high selectivity probe: 5% of the rows match"),
 ("sel_01",             "AnDim512K",  "inner",     "int",     "narrow", "0.001:1","none",   "very high selectivity probe on a big build side"),
 ("dups4_inner",        "AnDim32K",   "inner",     "int",     "narrow", "1:4",    "none",   "four build rows per key"),
 ("left_dim32k",        "AnDim32K",   "left",      "int",     "narrow", "0.9:1",  "none",   "fact LEFT JOIN dimension, 10% miss"),
 ("left_dim512k_wide",  "AnDim512K",  "left",      "int",     "wide",   "0.9:1",  "none",   "fact LEFT JOIN big dimension, wide rows"),
 ("semi_dim32k",        "AnDim32K",   "left-semi", "int",     "narrow", "0.5:1",  "none",   "EXISTS on a small set"),
 ("semi_dim512k",       "AnDim512K",  "left-semi", "int",     "narrow", "0.5:1",  "none",   "EXISTS on a big set"),
 ("only_dim32k",        "AnDim32K",   "left-only", "int",     "narrow", "0.1:1",  "none",   "NOT EXISTS on a small set"),
 ("only_dim512k",       "AnDim512K",  "left-only", "int",     "narrow", "0.9:1",  "none",   "NOT EXISTS on a big set, most rows survive"),
 ("semi_filter",        "AnDim32K",   "left-semi", "int",     "narrow", "0.5:4",  "common", "EXISTS with an extra predicate"),
 ("str_dim32k",         "AnDim32K",   "inner",     "string",  "narrow", "1:1",    "none",   "string key, small dimension"),
 ("str_dim512k",        "AnDim512K",  "inner",     "string",  "narrow", "1:1",    "none",   "string key, big dimension"),
 ("str_left_dim512k",   "AnDim512K",  "left",      "string",  "narrow", "0.9:1",  "none",   "string key, left join, big dimension"),
 ("two_int_keys",       "AnDim32K",   "inner",     "int,int", "narrow", "1:1",    "none",   "composite integer key"),
 ("int_string_keys",    "AnDim32K",   "inner",     "int,string", "narrow", "1:1", "none",   "composite integer and string key"),
 ("chain_1",            "AnChain1",   "inner",     "int",     "narrow", "1:1",    "none",   "join chain, stage 1: fact JOIN tiny dimension"),
 ("chain_2",            "AnChain2",   "inner",     "int",     "narrow", "0.5:1",  "none",   "join chain, stage 2: half of the rows survive stage 1"),
 ("chain_3",            "AnChain3",   "left",      "int",     "wide",   "0.9:1",  "none",   "join chain, stage 3: smaller probe, big dimension, left join"),
]

def run_case(binary, presets, c, samples, algo, scale):
    name, preset, kind, ks, payload, sel, flt, _ = c
    cmd = [binary, "--presets-path", presets, "--preset", preset, "--preset-flavour", "same-size",
           "--scale", str(scale), "--samples", str(samples), "--warmup", "1", "--algo", algo,
           "--key-schema", ks, "--payload", payload, "--selectivity", sel, "--join-kind", kind,
           "--filter", flt, "--block-size", BLOCKS]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError("%s failed:\n%s" % (" ".join(cmd), p.stderr[-2000:]))
    out = [json.loads(l) for l in p.stdout.splitlines() if l.startswith("{")]
    return name, out[0], " ".join(cmd)

def cmd_run(a):
    here = os.path.dirname(os.path.abspath(__file__))
    cases = [c for c in CASES if not a.only or any(o in c[0] for o in a.only)]
    res = {}
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        futs = [ex.submit(run_case, a.binary, a.presets or here, c, a.samples, a.algo, a.scale) for c in cases]
        for f in futs:
            name, r, cmd = f.result()
            res[name] = {"medianCpuUs": r["medianCpuUs"], "minCpuUs": r["minCpuUs"], "cv": r["cvPercent"],
                         "outputRows": r["outputRows"], "leftRows": r["leftRows"], "rightRows": r["rightRows"],
                         "cmd": cmd}
            print("%-22s %8.1f ms (min %.1f, cv %.1f%%) out=%d" % (name, r["medianCpuUs"]/1e3, r["minCpuUs"]/1e3, r["cvPercent"], r["outputRows"]), file=sys.stderr)
    json.dump(res, open(a.output, "w"), indent=1)
    tot = sum(v["medianCpuUs"] for v in res.values())/1e3
    chain = sum(v["medianCpuUs"] for k, v in res.items() if k.startswith("chain_"))/1e3
    print("total median cpu: %.1f ms, join chain (chain_1..3): %.1f ms" % (tot, chain), file=sys.stderr)

def mrows(r):
    return r["leftRows"] / (r["medianCpuUs"] / 1e6) / 1e6

def cmd_table(a):
    r = json.load(open(a.files[0]))
    print("| case | probe rows | build rows | median ms | Mrows/s (probe) | cv % |\n|---|---|---|---|---|---|")
    for k, v in r.items():
        print("| %s | %d | %d | %.1f | %.2f | %.1f |" % (k, v["leftRows"], v["rightRows"], v["medianCpuUs"]/1e3, mrows(v), v["cv"]))

def cmd_compare(a):
    b, n = json.load(open(a.files[0])), json.load(open(a.files[1]))
    print("| case | before ms | after ms | speedup | out rows |\n|---|---|---|---|---|")
    tb = tn = 0
    for k in b:
        if k not in n: continue
        x, y = b[k]["medianCpuUs"]/1e3, n[k]["medianCpuUs"]/1e3
        tb += x; tn += y
        ok = "same" if b[k]["outputRows"] == n[k]["outputRows"] else "MISMATCH"
        print("| %s | %.1f | %.1f | %.2fx | %s |" % (k, x, y, x/y, ok))
    print("| **total** | %.1f | %.1f | %.2fx | |" % (tb, tn, tb/tn))

def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    r = sp.add_parser("run"); r.add_argument("--binary", default="./join_perf"); r.add_argument("-o", "--output", required=True)
    r.add_argument("--jobs", type=int, default=1); r.add_argument("--only", action="append"); r.add_argument("--samples", type=int, default=5)
    r.add_argument("--algo", default="block-hash"); r.add_argument("--scale", type=int, default=1); r.add_argument("--presets"); r.set_defaults(f=cmd_run)
    t = sp.add_parser("table"); t.add_argument("files", nargs=1); t.set_defaults(f=cmd_table)
    c = sp.add_parser("compare"); c.add_argument("files", nargs=2); c.set_defaults(f=cmd_compare)
    a = ap.parse_args(); a.f(a)

if __name__ == "__main__":
    main()
