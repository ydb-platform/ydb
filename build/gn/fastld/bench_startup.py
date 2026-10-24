#!/usr/bin/env python3
"""Startup time of test binaries natively vs via a launcher (fastld).

  bench_startup.py [--build-dir DIR] [--launcher CMD] [-n 2] BIN|GLOB...

Each binary runs `<bin> --list-verbose` natively and as `CMD <bin> --list-verbose`,
N times each, interleaved and strictly one at a time. The time shown is the best
of N; "same list" compares stdout and exit codes of all runs.
BIN|GLOB is a path or a name/glob relative to the build dir (ydb-core-kqp-ut-*).
"""

import argparse
import glob
import os
import shlex
import subprocess
import sys
import time

DEFAULT_LAUNCHER = ("/place/vartmp/claude-25341/-home-ilezhankin-g-ydb-gn/"
                    "68912edb-cd26-452f-b8f7-b5c5efe0a8f7/scratchpad/fastld/fastld")


def run(argv, cwd, env, timeout):
    """(seconds, rc, stdout bytes, stderr bytes); rc None on a timeout."""
    t = time.monotonic()
    try:
        p = subprocess.run(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        return time.monotonic() - t, p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return time.monotonic() - t, None, b"", b""


def binaries(build_dir, specs):
    res = []
    for s in specs:
        pat = s if os.path.isabs(s) or s.startswith("./") else os.path.join(build_dir, s)
        found = sorted(p for p in glob.glob(pat) if os.path.isfile(p) and os.access(p, os.X_OK))
        if not found:
            sys.exit("no binaries match %s" % s)
        res += [p for p in found if p not in res]
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-dir", default=os.path.expanduser("~/b/gn"))
    ap.add_argument("--launcher", default=DEFAULT_LAUNCHER, help="command, shell-split")
    ap.add_argument("-n", "--repeat", type=int, default=2)
    ap.add_argument("--timeout", type=float, default=600, help="seconds per run")
    ap.add_argument("--stats", action="store_true", help="FASTLD_STATS=1, print its lines of the last run")
    ap.add_argument("specs", nargs="+", metavar="BIN|GLOB")
    args = ap.parse_args()
    build_dir = os.path.realpath(args.build_dir)
    launcher = shlex.split(args.launcher)
    bins = binaries(build_dir, args.specs)
    env = dict(os.environ, ARCADIA_BUILD_ROOT=build_dir)
    if args.stats:
        env["FASTLD_STATS"] = "1"

    rows = []
    hdr = "%-50s %9s %9s %8s %5s" % ("binary", "native s", "fastld s", "speedup", "same")
    print(hdr + "\n" + "-" * len(hdr), flush=True)
    for b in bins:
        native, fast, outs, notes = [], [], set(), []
        for _ in range(args.repeat):
            for argv, times in (([b], native), (launcher + [b], fast)):
                sec, rc, out, err = run(argv + ["--list-verbose"], build_dir, env, args.timeout)
                times.append(sec)
                outs.add((rc, out))
                if rc != 0:
                    notes.append("%s: %s" % ("fastld" if times is fast else "native",
                                             "timeout" if rc is None else "rc=%d" % rc))
                last_err = err
        tn, tf = min(native), min(fast)
        same = len(outs) == 1
        rows.append((tn, tf, same))
        print("%-50s %9.2f %9.2f %7.1fx %5s%s" % (os.path.basename(b), tn, tf, tn / tf if tf else 0,
                                                 "yes" if same else "NO",
                                                 "  (" + ", ".join(sorted(set(notes))) + ")" if notes else ""),
              flush=True)
        if args.stats:
            for l in last_err.decode(errors="replace").splitlines():
                if l.startswith("fastld:"):
                    print("    " + l)

    tn, tf = sum(r[0] for r in rows), sum(r[1] for r in rows)
    print("-" * len(hdr))
    print("%-50s %9.2f %9.2f %7.1fx %5s" % ("total (%d binaries, best of %d)" % (len(rows), args.repeat),
                                           tn, tf, tn / tf if tf else 0,
                                           "%d/%d" % (sum(r[2] for r in rows), len(rows))))
    return 0 if all(r[2] for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
