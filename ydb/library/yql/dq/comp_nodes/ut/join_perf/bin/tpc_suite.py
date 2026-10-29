#!/usr/bin/env python3
import argparse, json, os, subprocess, sys, concurrent.futures as cf

BLOCKS = "8192,6000,8192,2048"

CASES = [
 ("h_lineitem_supplier",   "TpchLineitemSupplier",       "inner",     "int",         "tpc",    "1:1",     "none",   "Q5/Q7/Q9: lineitem JOIN supplier, tiny build, every row matches"),
 ("h_lineitem_orders_f",   "TpchLineitemOrdersFiltered", "inner",     "int",         "tpc",    "0.25:1",  "none",   "Q3/Q10: lineitem JOIN filtered orders, 25% survive"),
 ("h_lineitem_orders_w",   "TpchLineitemOrdersFiltered", "inner",     "int",         "wide",   "1:1",     "none",   "Q18-like: wide fact rows, 100% match, 256K build"),
 ("h_orders_semi",         "TpchOrdersLineitem",         "left-semi", "int",         "tpc",    "0.9:4",   "none",   "Q4/Q18/Q21 EXISTS: orders LEFT SEMI lineitem"),
 ("h_orders_semi_filter",  "TpchOrdersLineitem",         "left-semi", "int",         "tpc",    "0.9:4",   "common", "Q21: EXISTS with non-equi predicate"),
 ("h_orders_only_filter",  "TpchOrdersLineitem",         "left-only", "int",         "tpc",    "0.9:4",   "common", "Q21: NOT EXISTS with non-equi predicate"),
 ("h_customer_orders_left","TpchOrdersLineitem",         "left",      "int",         "tpc",    "0.67:10", "none",   "Q13: customer LEFT JOIN orders, ~10 dups"),
 ("h_partsupp_anti_small", "TpchPartsuppSupplier",       "left-only", "int",         "tpc",    "0.02:1",  "none",   "Q16: NOT IN (tiny supplier set)"),
 ("h_customer_anti",       "TpchOrdersLineitem",         "left-only", "int",         "narrow", "0.33:4",  "none",   "Q22: customer NOT EXISTS orders"),
 ("ds_fact_item",          "TpcdsFactItem",              "inner",     "int32",       "tpc",    "1:1",     "none",   "store_sales JOIN item (int32 surrogate key)"),
 ("ds_fact_date_f",        "TpcdsFactDateFiltered",      "inner",     "int32",       "tpc",    "0.25:1",  "none",   "store_sales JOIN date_dim filtered to a year"),
 ("ds_fact_customer_n",    "TpcdsFactCustomer",          "inner",     "nint32",      "tpc",    "1:1",     "none",   "store_sales JOIN customer on nullable ss_customer_sk"),
 ("ds_fact_customer_left", "TpcdsFactCustomer",          "left",      "nint32",      "tpc",    "0.9:1",   "none",   "web_sales LEFT JOIN customer, nullable key, 10% miss"),
 ("ds_sales_returns_left", "TpcdsSalesReturns",          "left",      "int32,int32", "tpc",    "0.09:1",  "none",   "Q17/Q25/Q29/Q78: sales LEFT JOIN returns (ticket,item)"),
 ("ds_sales_returns_3k",   "TpcdsSalesReturns",          "inner",     "int32,int32,int32", "tpc", "0.09:1", "none",  "Q78: composite 3 int key, 9% match"),
 ("ds_customer_address_s", "TpcdsCustomerAddress",       "inner",     "string",      "tpc",    "1:1",     "none",   "customer JOIN customer_address on string key"),
 ("ds_customer_address_ns","TpcdsCustomerAddress",       "left",      "nstring",     "tpc",    "0.9:1",   "none",   "nullable string key left join"),
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
            print("%-26s %8.1f ms (min %.1f, cv %.1f%%) out=%d" % (name, r["medianCpuUs"]/1e3, r["minCpuUs"]/1e3, r["cvPercent"], r["outputRows"]), file=sys.stderr)
    json.dump(res, open(a.output, "w"), indent=1)
    tot = sum(v["medianCpuUs"] for v in res.values())/1e3
    print("total median cpu: %.1f ms" % tot, file=sys.stderr)

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

ap = argparse.ArgumentParser()
sp = ap.add_subparsers(dest="cmd", required=True)
r = sp.add_parser("run"); r.add_argument("--binary", default="./join_perf"); r.add_argument("-o", "--output", required=True)
r.add_argument("--jobs", type=int, default=1); r.add_argument("--only", action="append"); r.add_argument("--samples", type=int, default=5)
r.add_argument("--algo", default="block-hash"); r.add_argument("--scale", type=int, default=1); r.add_argument("--presets"); r.set_defaults(f=cmd_run)
t = sp.add_parser("table"); t.add_argument("files", nargs=1); t.set_defaults(f=cmd_table)
c = sp.add_parser("compare"); c.add_argument("files", nargs=2); c.set_defaults(f=cmd_compare)
a = ap.parse_args(); a.f(a)
