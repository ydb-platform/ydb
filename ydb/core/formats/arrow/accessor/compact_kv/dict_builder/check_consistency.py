#!/usr/bin/env python3
"""Round-trip consistency check for the per-column dict_builder.

For every matching sample file it runs:

    builder  --input SAMPLE --output tmp.bin
    reader   --input tmp.bin --output tmp.ndjson
    checker  --file1 SAMPLE  --file2 tmp.ndjson

and reports per-file OK/FAIL. The checker compares JSON-equivalence (sorted
keys, whitespace-insensitive), so a pass means the encode/decode round trip
preserved every row.

Defaults wire up dict_builder + dict_reader, but --builder/--reader let you
point the same harness at compact_kv's builder + reader instead.

Examples:
    check_consistency.py                       # dict_builder round-trip, *_50k
    check_consistency.py --glob labels --glob meta
    check_consistency.py \
        --builder ../builder/builder --reader ../reader/reader
"""

import argparse
import concurrent.futures
import fnmatch
import os
import subprocess
import sys
import tempfile

DEFAULT_SAMPLES_DIR = "/home/risenberg/compact_kv/samples"

HERE = os.path.dirname(os.path.abspath(__file__))          # .../compact_kv/dict_builder
CKV = os.path.dirname(HERE)                                # .../compact_kv


def default(path, fallback):
    return path if os.path.exists(path) else fallback


def discover_samples(root, patterns):
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for f in sorted(files):
            if any(fnmatch.fnmatch(f, p) for p in patterns):
                full = os.path.join(dirpath, f)
                out.append((os.path.relpath(full, root), full))
    out.sort()
    return out


def run(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def check_one(builder, reader, checker, name, path, zstd_level, tmpdir):
    base = os.path.join(tmpdir, name.replace(os.sep, "_"))
    out_bin = base + ".bin"
    out_ndjson = base + ".ndjson"
    try:
        b = run([builder, "--input", path, "--output", out_bin,
                 "--zstd-level", str(zstd_level)])
        if b.returncode != 0:
            return (name, False, "builder: " + (b.stderr.strip() or "nonzero exit"))
        r = run([reader, "--input", out_bin, "--output", out_ndjson])
        if r.returncode != 0:
            return (name, False, "reader: " + (r.stderr.strip() or "nonzero exit"))
        c = run([checker, "--file1", path, "--file2", out_ndjson])
        if c.returncode != 0:
            return (name, False, "checker: " + (c.stderr.strip() or "mismatch"))
        return (name, True, (c.stderr.strip() or "ok"))
    finally:
        for p in (out_bin, out_ndjson):
            try:
                os.remove(p)
            except OSError:
                pass


def main():
    ap = argparse.ArgumentParser(
        description="Round-trip consistency check via compact_kv/checker.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--samples-dir", default=DEFAULT_SAMPLES_DIR)
    ap.add_argument("--glob", action="append", dest="globs",
                    help="glob(s) selecting sample files (default: '*_50k')")
    ap.add_argument("--builder", default=os.path.join(HERE, "dict_builder"),
                    help="encoder binary")
    ap.add_argument("--reader", default=os.path.join(CKV, "dict_reader", "dict_reader"),
                    help="decoder binary")
    ap.add_argument("--checker", default=os.path.join(CKV, "checker", "checker"),
                    help="NDJSON equivalence checker")
    ap.add_argument("--zstd-level", type=int, default=4)
    ap.add_argument("--jobs", type=int, default=os.cpu_count())
    args = ap.parse_args()

    for tool in (args.builder, args.reader, args.checker):
        if not os.path.exists(tool):
            print(f"Missing binary: {tool}\n(build it with `ya make -r`)", file=sys.stderr)
            return 2

    globs = args.globs or ["*_50k"]
    samples = discover_samples(args.samples_dir, globs)
    if not samples:
        print(f"No samples matching {globs} under {args.samples_dir}", file=sys.stderr)
        return 1

    print(f"Checking {len(samples)} sample(s): {os.path.basename(args.builder)} -> "
          f"{os.path.basename(args.reader)} -> {os.path.basename(args.checker)}, "
          f"jobs={args.jobs}\n", file=sys.stderr)

    results = []
    with tempfile.TemporaryDirectory(prefix="ckv_chk_") as tmpdir:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
            futs = [ex.submit(check_one, args.builder, args.reader, args.checker,
                              name, path, args.zstd_level, tmpdir)
                    for name, path in samples]
            for fut in concurrent.futures.as_completed(futs):
                results.append(fut.result())

    name_w = max(len(r[0]) for r in results)
    n_ok = 0
    for name, ok, msg in sorted(results):
        if ok:
            n_ok += 1
            print(f"  OK   {name:<{name_w}}  {msg}")
        else:
            print(f"  FAIL {name:<{name_w}}  {msg}")

    print(f"\n{n_ok}/{len(results)} passed")
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
