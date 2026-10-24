#!/usr/bin/env python3
"""Compare glibc LD_DEBUG=bindings output with a fastld FASTLD_BINDINGS dump.

  compare_bindings.py --glibc bind.PID --fastld fastld.tsv [--cwd DIR] [--build-dir DIR]

Only (requester, symbol) pairs whose requester lives under --build-dir are
compared. Exit status is 0 when no definer or version mismatches are found.
"""

import argparse
import os
import re
import sys

BIND_RE = re.compile(
    r"binding file (.+?) \[\d+\] to (.+?) \[\d+\]: [a-z ]+? symbol `([^']*)'(?: \[([^\]]*)\])?")


class Normalizer:
    """realpath with a cache; names without '/' (linux-vdso.so.1) stay as is."""

    def __init__(self, cwd):
        self.cwd = cwd
        self.cache = {}

    def __call__(self, path):
        r = self.cache.get(path)
        if r is None:
            r = os.path.realpath(os.path.join(self.cwd, path)) if "/" in path else path
            self.cache[path] = sys.intern(r)
        return r


def read_glibc(path, norm, in_build):
    """{(requester, symbol): (set of definers, set of versions)} for build-dir requesters."""
    res = {}
    lines = 0
    with open(path, errors="replace") as f:
        for line in f:
            if "binding file" not in line:
                continue
            m = BIND_RE.search(line)
            if not m:
                continue
            lines += 1
            req = norm(m.group(1))
            if not in_build(req):
                continue
            key = (req, m.group(3))
            ent = res.get(key)
            if ent is None:
                ent = res[key] = (set(), set())
            # Same pair bound twice: a copy reloc next to a GLOB_DAT, or two
            # references of different versions (memcpy@GLIBC_2.14 and @GLIBC_2.2.5).
            ent[0].add(norm(m.group(2)))
            ent[1].add(m.group(4) or "")
    return res, lines


def read_fastld(path, norm, in_build):
    """{(requester, symbol): (set of definers or '', set of versions)} for build-dir requesters."""
    res = {}
    with open(path, errors="replace") as f:
        for n, line in enumerate(f, 1):
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 4:
                sys.exit("%s:%d: expected 4 TSV fields, got %d" % (path, n, len(parts)))
            req, sym, ver, definer = parts
            req = norm(req)
            if in_build(req):
                ent = res.setdefault((req, sym), (set(), set()))
                ent[0].add(norm(definer) if definer else "")
                ent[1].add(ver)
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--glibc", required=True, help="LD_DEBUG=bindings output file")
    ap.add_argument("--fastld", required=True, help="FASTLD_BINDINGS dump (TSV)")
    ap.add_argument("--cwd", default=os.getcwd(), help="cwd of the glibc run, for relative paths")
    ap.add_argument("--build-dir", default=os.path.expanduser("~/b/gn"))
    ap.add_argument("--show", type=int, default=50, help="examples to print per category")
    args = ap.parse_args()

    norm = Normalizer(args.cwd)
    prefix = os.path.realpath(args.build_dir).rstrip("/") + "/"
    in_build = lambda p: p.startswith(prefix)
    short = lambda p: p[len(prefix):] if p.startswith(prefix) else p

    glibc, glibc_lines = read_glibc(args.glibc, norm, in_build)
    fast = read_fastld(args.fastld, norm, in_build)

    matches = multi = 0
    mismatches, ver_mismatches, only_glibc = [], [], []
    for key, (defs, ver) in glibc.items():
        f = fast.pop(key, None)
        if f is None:
            only_glibc.append(key)
            continue
        fdefs, fver = f
        if len(defs) > 1:
            multi += 1
        if not fdefs <= defs:
            mismatches.append((key, sorted(defs), sorted(fdefs)))
        elif fver != ver:
            ver_mismatches.append((key, sorted(ver), sorted(fver)))
        else:
            matches += 1
    # What is left in `fast` has no glibc counterpart.
    only_fast_weak = [k for k, (d, _) in fast.items() if d == {""}]
    only_fast = [k for k, (d, _) in fast.items() if d != {""}]

    print("glibc binding lines:          %d" % glibc_lines)
    print("glibc pairs (build-dir req):  %d (with >1 definer: %d)"
          % (len(glibc), sum(1 for d, _ in glibc.values() if len(d) > 1)))
    print("fastld pairs (build-dir req): %d" % (len(glibc) - len(only_glibc) + len(fast)))
    print("matches:                      %d (of them multi-definer in glibc: %d)" % (matches, multi))
    print("definer mismatches:           %d" % len(mismatches))
    print("version mismatches:           %d" % len(ver_mismatches))
    print("only in glibc:                %d" % len(only_glibc))
    print("only in fastld, resolved:     %d" % len(only_fast))
    print("only in fastld, weak unres.:  %d" % len(only_fast_weak))

    def show(title, items, fmt):
        if items:
            print("\n%s (first %d of %d):" % (title, min(args.show, len(items)), len(items)))
            for it in items[:args.show]:
                print("  " + fmt(it))

    show("definer mismatches", mismatches, lambda it: "%s %s\n    glibc:  %s\n    fastld: %s" % (
        short(it[0][0]), it[0][1], " | ".join(map(short, it[1])), " | ".join(short(d) or "<unresolved>" for d in it[2])))
    show("version mismatches", ver_mismatches, lambda it: "%s %s glibc=%s fastld=%s" % (
        short(it[0][0]), it[0][1], it[1], it[2]))
    show("only in glibc", only_glibc, lambda k: "%s %s -> %s" % (
        short(k[0]), k[1], " | ".join(map(short, sorted(glibc[k][0])))))
    show("only in fastld, resolved", only_fast, lambda k: "%s %s -> %s" % (
        short(k[0]), k[1], " | ".join(map(short, sorted(fast[k][0])))))

    return 1 if mismatches or ver_mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
