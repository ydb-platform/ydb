#!/usr/bin/env python3
"""Measures the incremental build of one commit by GN/ninja and by ya make (the
bench stage of the GN follow-up, see follow_main.py): checks the commit out in
the bench work tree -- moving it there from the previous one, nothing else
touches that tree -- and builds the same targets both ways, one after another:

  ninja  gn gen + ninja <targets> in the bench build dir (its env: the compiler
         cache tag of the bench, e.g. DC_TAGS)
  ya     ya make -R --no-yt-store --no-bazel-remote-store of --ya-dirs (else
         of the dirs of the labels of those groups of the root BUILD.gn), with
         its own local cache (--ya-cache-root, the tools stay in ~/.ya/tools)

--warm: before moving, ya builds the --ya-dirs (those of them that exist there)
in the previous commit, not measured: ya builds only some targets each time,
and its cache must hold them as of the previous commit -- as the tree of a
developer who works on them -- for the measurement to cost the commit alone.
Only the previous commit's results get into the cache, nothing of this one.

Measured: wall time; CPU time of the whole machine (/proc/stat: the compiler
cache daemon compiles, not a child of the build -- the machine must be idle,
the load is checked first); ninja: the commands run, compiles and links of
them (.ninja_log), the compiler cache counters (dist-clang --stats --raw);
ya: its graph and build stages, the nodes run by kind and the ones restored
from the local cache (its evlog).

    follow_bench.py --root <bench work tree> --out <bench build dir>
                    --ya-out <ya output dir> --commit <rev> --result <json>
                    [--ya-cache-root DIR] [--ya-cache-size 120GiB] [--order ninja,ya]
                    [--targets all tests] [--ya-dirs DIR ... [--warm]]
                    [--env NAME=VALUE ...] [--max-load X] [--full]

--full: the first build of a series (warms the caches; the same measurements).
The result JSON has "trailers": the lines follow_main.py adds to the commit
message. Exit code: 0, or 1 if a build failed (the result says which).
"""

import argparse
import collections
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time

DEFAULT_TARGETS = ["all", "tests"]
STATUS_RE = re.compile(r"^\[(\d+)/(\d+)\] ")


def say(text):
    print("%s   bench: %s" % (time.strftime("%H:%M:%S"), text), flush=True)


def cpu_busy():
    """Busy CPU seconds of the machine so far (all CPUs)."""
    with open("/proc/stat") as f:
        v = [int(x) for x in f.readline().split()[1:9]]
    return (v[0] + v[1] + v[2] + v[5] + v[6] + v[7]) / os.sysconf("SC_CLK_TCK")


def wait_idle(max_load, timeout):
    """Waits for the 1-minute load to go below max_load; the load seen."""
    deadline = time.time() + timeout
    while True:
        load = os.getloadavg()[0]
        if load <= max_load or time.time() > deadline:
            return load
        say("load %.1f > %.1f, waiting" % (load, max_load))
        time.sleep(30)


def git(root, *args):
    return subprocess.run(["git", "-C", root] + list(args), stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, universal_newlines=True, check=True).stdout.strip()


# ---- targets --------------------------------------------------------------------

def target_labels(root, targets):
    """The labels listed in the groups of the root BUILD.gn named by the ninja
    targets; a target of no such group stands for itself (a dir or dir:name)."""
    with open(os.path.join(root, "BUILD.gn")) as f:
        text = f.read()
    labels = []
    for name in targets:
        m = re.search(r'^group\("%s"\)\s*\{(.*?)^\}' % re.escape(name), text, re.S | re.M)
        labels += re.findall(r'^\s*"(//[^"]+)"', m.group(1), re.M) if m else ["//" + name]
    return labels


def ya_dirs(root, labels):
    """The module dirs of the labels, for ya make."""
    dirs = set()
    for label in labels:
        d = label[2:].split(":")[0]
        if os.path.exists(os.path.join(root, d, "ya.make")):
            dirs.add(d)
    return sorted(dirs)


# ---- ninja ----------------------------------------------------------------------

def dc_stats():
    """{counter: value} of dist-clang, or {} without it."""
    if not shutil.which("dist-clang"):
        return {}
    r = subprocess.run(["dist-clang", "--stats", "--raw"], stdout=subprocess.PIPE,
                       stderr=subprocess.DEVNULL, universal_newlines=True)
    out = {}
    for line in r.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return out


def log_entries(path, offset):
    """The outputs of the .ninja_log entries after offset (the whole log if
    ninja recompacted it), deduplicated by command."""
    with open(path) as f:
        f.seek(offset if os.path.getsize(path) >= offset else 0)
        seen, outs = set(), []
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) == 5 and (p[0], p[1], p[4]) not in seen:
                seen.add((p[0], p[1], p[4]))
                outs.append(p[3])
    return outs


def run_ninja(args, env, log_dir):
    res = {}
    t = time.time()
    with open(os.path.join(log_dir, "bench_gn_gen.log"), "w") as f:
        rc = subprocess.run(["gn", "gen", "--script-executable=" + args.script_executable, args.out],
                            cwd=args.root, stdout=f, stderr=subprocess.STDOUT, env=env).returncode
    res["gen"] = round(time.time() - t, 1)
    if rc != 0:
        return dict(res, rc=rc, failed="gn gen")
    ninja_log = os.path.join(args.out, ".ninja_log")
    offset = os.path.getsize(ninja_log) if os.path.exists(ninja_log) else 0
    dc0, c0, t = dc_stats(), cpu_busy(), time.time()
    log = os.path.join(log_dir, "bench_ninja.log")
    with open(log, "w") as f:
        rc = subprocess.run(["ninja", "-C", args.out] + args.targets, stdout=f, stderr=subprocess.STDOUT,
                            env=dict(env, NINJA_STATUS="[%f/%t] ")).returncode
    res.update(rc=rc, wall=round(time.time() - t, 1), cpu=round(cpu_busy() - c0))
    actions = 0
    with open(log) as f:
        for line in f:
            m = STATUS_RE.match(line)
            if m:
                actions = int(m.group(1))
    outs = log_entries(ninja_log, offset) if os.path.exists(ninja_log) else []
    res["actions"] = actions
    res["compiles"] = sum(1 for o in outs if o.endswith(".o"))
    # shared libraries, and executables (in root_out_dir, no extension)
    res["links"] = sum(1 for o in outs if o.endswith(".so") or ("/" not in o and "." not in o))
    dc1 = dc_stats()
    res["dist_clang"] = {k: dc1[k] - dc0.get(k, 0) for k in dc1 if dc1[k] != dc0.get(k, 0)}
    if rc != 0:
        res["failed"] = "ninja"
    stats = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build_stats.py")
    with open(os.path.join(log_dir, "bench_build_stats.log"), "w") as f:
        subprocess.run([args.python, stats, "record", args.out, "--status", str(rc), "--label", "follow",
                        "--no-traces"], cwd=args.root, stdout=f, stderr=subprocess.STDOUT, env=env)
    return res


# ---- ya -------------------------------------------------------------------------

def find_evlog(ya_root, since):
    evlogs = os.path.join(ya_root, "evlogs")
    found = []
    for dirpath, _, files in os.walk(evlogs):
        for name in files:
            path = os.path.join(dirpath, name)
            if name.endswith(".evlog.zst") and os.path.getmtime(path) >= since:
                found.append((os.path.getmtime(path), path))
    return max(found)[1] if found else None


def parse_evlog(path):
    stages, run, run_time = {}, collections.Counter(), collections.Counter()
    restored, dist_found = 0, 0
    p = subprocess.Popen(["zstd", "-qdc", path], stdout=subprocess.PIPE, universal_newlines=True,
                         errors="replace")
    for line in p.stdout:
        try:
            e = json.loads(line)
        except ValueError:
            continue
        ns, ev, v = e.get("namespace"), e.get("event"), e.get("value") or {}
        if ns == "stages" and ev == "stage-finished" and "time" in v:
            stages[v.get("name")] = v["time"][1] - v["time"][0]
        elif ns == "worker_threads" and ev == "node-finished":
            tag = v.get("tag") or ""
            if v.get("name", "").startswith("Run("):
                run[tag] += 1
                run_time[tag] += v["time"][1] - v["time"][0]
            elif tag.startswith("restore["):
                restored += 1
        elif ns == "yt_store" and ev == "stats":
            dist_found = (v.get("cache_hit") or {}).get("found", 0)
    p.wait()
    return {"graph": round(stages.get("build_graph_and_tests", 0), 1),
            "build": round(stages.get("dispatch_build", 0), 1),
            "total": round(stages.get("ya_make_handler", 0), 1),
            "executed": sum(run.values()), "executed_by_kind": dict(run),
            "executed_seconds": {k: round(s, 1) for k, s in run_time.items()},
            "local_hits": restored, "dist_hits": dist_found}


def run_ya(args, dirs, log_dir, name="bench_ya"):
    env = dict(os.environ)
    if args.ya_cache_root:
        env["YA_CACHE_DIR"] = args.ya_cache_root
        env.setdefault("YA_CACHE_DIR_TOOLS", os.path.expanduser("~/.ya/tools"))
    ya_root = env.get("YA_CACHE_DIR") or os.path.expanduser("~/.ya")
    cmd = [os.path.join(args.root, "ya"), "make", "--build=" + args.ya_build, "--no-src-links",
           "-o", args.ya_out, "--no-yt-store", "--no-bazel-remote-store", "-R", "--stat"] \
        + (["--cache-size=" + args.ya_cache_size] if args.ya_cache_size else []) + dirs
    with open(os.path.join(log_dir, name + ".cmd"), "w") as f:
        f.write(" ".join(cmd) + "\n")
    c0, t = cpu_busy(), time.time()
    with open(os.path.join(log_dir, name + ".log"), "w") as f:
        rc = subprocess.run(cmd, cwd=args.root, stdout=f, stderr=subprocess.STDOUT, env=env).returncode
    res = {"rc": rc, "wall": round(time.time() - t, 1), "cpu": round(cpu_busy() - c0)}
    evlog = find_evlog(ya_root, t - 1)
    if evlog:
        res["evlog"] = evlog
        res.update(parse_evlog(evlog))
    if rc != 0:
        res["failed"] = "ya"
    return res


# ---- trailers -------------------------------------------------------------------

def trailers(r):
    n, y, h = r.get("ninja", {}), r.get("ya", {}), r["host"]
    dc = n.get("dist_clang", {})
    hits = dc.get("DIRECT_CACHE_HIT", 0) + dc.get("SIMPLE_CACHE_HIT", 0)
    out = []
    if n:
        out.append("GN-Bench-Ninja: gen=%ss wall=%ss cpu=%ss actions=%s compiles=%s links=%s cache-hits=%d "
                   "direct-misses=%d" % (n.get("gen"), n.get("wall"), n.get("cpu"), n.get("actions"),
                                         n.get("compiles"), n.get("links"), hits,
                                         dc.get("DIRECT_CACHE_MISS", 0)))
    if y:
        k = y.get("executed_by_kind", {})
        out.append("GN-Bench-Ya: graph=%ss wall=%ss cpu=%ss executed=%s compiles=%d links=%d local-hits=%s "
                   "dist-hits=%s" % (y.get("graph"), y.get("wall"), y.get("cpu"), y.get("executed"),
                                     k.get("CC", 0) + k.get("AS", 0), k.get("LD", 0), y.get("local_hits"),
                                     y.get("dist_hits")))
    if r.get("ya_dirs"):
        d = r["ya_dirs"]
        out.append("GN-Bench-Ya-Targets: %s" % (" ".join(d) if len(d) <= 6 else "%s ... (%d)" % (" ".join(d[:5]), len(d))))
    out.append("GN-Bench-Host: host=%s nproc=%d load=%.2f order=%s from=%s%s" % (
        h["host"], h["nproc"], h["load"], ",".join(r["order"]), r["from"][:11],
        " full" if r["full"] else ""))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True, help="the bench work tree")
    ap.add_argument("--out", required=True, help="the bench build dir (args.gn there)")
    ap.add_argument("--ya-out", required=True, help="ya make -o")
    ap.add_argument("--commit", required=True)
    ap.add_argument("--result", required=True, help="JSON written here")
    ap.add_argument("--ya-cache-root", help="YA_CACHE_DIR of the bench (default: ~/.ya)")
    ap.add_argument("--ya-cache-size", help="ya make --cache-size, e.g. 100GiB (default: ya's)")
    ap.add_argument("--ya-build", default="relwithdebinfo")
    ap.add_argument("--order", default="ninja,ya")
    ap.add_argument("--targets", nargs="+", default=DEFAULT_TARGETS, help="of ninja")
    ap.add_argument("--ya-dirs", nargs="+", help="of ya make (default: the dirs of --targets)")
    ap.add_argument("--warm", action="store_true", help="ya builds --ya-dirs in the previous commit first")
    ap.add_argument("--env", action="append", default=[], metavar="NAME=VALUE", help="for gn gen and ninja")
    ap.add_argument("--max-load", type=float, help="default: 5%% of the CPUs")
    ap.add_argument("--idle-timeout", type=int, default=900)
    ap.add_argument("--full", action="store_true", help="the first build of a series")
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--script-executable", default=os.environ.get("GN_SCRIPT_EXECUTABLE", "python3.9"))
    args = ap.parse_args()
    args.root, args.out = os.path.realpath(args.root), os.path.realpath(args.out)
    args.ya_out = os.path.realpath(args.ya_out)
    if args.ya_cache_root:
        args.ya_cache_root = os.path.realpath(args.ya_cache_root)
    log_dir = os.path.dirname(os.path.realpath(args.result))
    os.makedirs(log_dir, exist_ok=True)
    env = dict(os.environ)
    for kv in args.env:
        name, _, value = kv.partition("=")
        env[name] = value

    if git(args.root, "status", "--porcelain"):
        sys.exit("follow_bench.py: the bench work tree has changes")
    prev = git(args.root, "rev-parse", "HEAD")
    warm = None
    if args.warm and args.ya_dirs:
        there = [d for d in args.ya_dirs if os.path.exists(os.path.join(args.root, d, "ya.make"))]
        say("ya warm-up in %s: %s" % (prev[:11], " ".join(there)))
        w = run_ya(args, there, log_dir, "bench_ya_warm")
        warm = {k: w.get(k) for k in ("rc", "wall", "executed", "executed_by_kind", "local_hits")}
        say("ya warm-up: rc %s, wall %ss" % (w.get("rc"), w.get("wall")))
    git(args.root, "checkout", "-q", "--detach", args.commit)
    order = args.order.split(",")
    nproc = os.cpu_count() or 1
    load = wait_idle(args.max_load if args.max_load is not None else nproc * 0.05, args.idle_timeout)
    result = {"commit": git(args.root, "rev-parse", "HEAD"), "from": prev, "full": args.full, "order": order,
              "env": args.env, "host": {"host": platform.node(), "nproc": nproc, "load": load}}
    dirs = args.ya_dirs or ya_dirs(args.root, target_labels(args.root, args.targets))
    missing = [d for d in dirs if not os.path.exists(os.path.join(args.root, d, "ya.make"))]
    dirs = [d for d in dirs if d not in missing]
    result.update(ya_dirs=dirs, ya_missing=missing, ya_warm=warm)
    rc = 0
    for side in order:
        say("%s %s" % (side, result["commit"][:11]))
        r = run_ninja(args, env, log_dir) if side == "ninja" else run_ya(args, dirs, log_dir)
        result[side] = r
        say("%s: rc %d, wall %ss" % (side, r.get("rc", -1), r.get("wall")))
        rc = rc or (r.get("rc") != 0)
    result["trailers"] = trailers(result)
    with open(args.result, "w") as f:
        json.dump(result, f, indent=1)
    return 1 if rc else 0


if __name__ == "__main__":
    sys.exit(main())
