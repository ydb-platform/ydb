#!/usr/bin/env python3
"""Estimate compact_kv compression over a directory of samples.

Each *sample* is a single input file (NDJSON / python-dict-repr, one row per
line). The script runs the `builder` tool on every matching sample in parallel,
collects its --json-stats output, and prints:

  * a per-sample table (rows / keys / values counts and raw->compressed sizes);
  * per-section aggregates (how well keys / values / rows compress overall);
  * averages over all samples.

Typical layout (one sub-dir per service, with subcolumn files inside):

    samples/
      api-proxy-critical/
        labels  labels_50k  meta  meta_50k
      billing-wallet/
        ...

Examples:
    # all *_50k subsets under the default samples dir, zstd level 4
    estimate_compression.py
    # the full files, level 9, 4 workers
    estimate_compression.py --glob '{labels,meta}' --zstd-level 9 --jobs 4
"""

import argparse
import concurrent.futures
import fnmatch
import json
import os
import subprocess
import sys
import tempfile

DEFAULT_SAMPLES_DIR = "/home/risenberg/compact_kv/samples"


def find_builder(explicit):
    if explicit:
        return explicit
    here = os.path.dirname(os.path.abspath(__file__))
    cand = os.path.join(here, "builder")
    if os.path.exists(cand):
        return cand
    return "builder"  # rely on PATH


def discover_samples(root, patterns):
    """Return [(name, path)] for files under root matching any glob pattern.

    name is the path relative to root (e.g. 'billing-wallet/meta_50k').
    """
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for f in sorted(files):
            if any(fnmatch.fnmatch(f, p) for p in patterns):
                full = os.path.join(dirpath, f)
                out.append((os.path.relpath(full, root), full))
    out.sort()
    return out


def run_one(builder, name, path, zstd_level, tmpdir):
    out_bin = os.path.join(tmpdir, name.replace(os.sep, "_") + ".bin")
    cmd = [
        builder,
        "--input", path,
        "--output", out_bin,
        "--zstd-level", str(zstd_level),
        "--json-stats",
    ]
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, text=True,
        )
        stats = json.loads(proc.stdout.strip().splitlines()[-1])
        stats["name"] = name
        return stats
    except subprocess.CalledProcessError as e:
        return {"name": name, "error": e.stderr.strip() or f"exit {e.returncode}"}
    except (json.JSONDecodeError, IndexError) as e:
        return {"name": name, "error": f"bad stats output: {e}"}
    finally:
        try:
            os.remove(out_bin)
        except OSError:
            pass


def ratio(raw, comp):
    return raw / comp if comp else 0.0


def human(n):
    # bytes -> short human string
    f = float(n)
    for unit in ("B", "K", "M", "G"):
        if abs(f) < 1024.0 or unit == "G":
            return f"{f:,.1f}{unit}" if unit != "B" else f"{int(f):,}{unit}"
        f /= 1024.0


def sec(r, name):
    for s in r["sections"]:
        if s["name"] == name:
            return s
    return {"name": name, "raw": 0, "compressed": 0}


def print_report(results):
    ok = [r for r in results if "error" not in r]
    bad = [r for r in results if "error" in r]

    if not ok:
        print("No successful samples.")
        for r in bad:
            print(f"  ERROR {r['name']}: {r['error']}")
        return

    # Section set is taken from the data, so the same report works for any
    # builder (compact_kv: keys/values/rows; dict_builder: keys/values/
    # presence/indexes).
    sec_names = [s["name"] for s in ok[0]["sections"]]
    name_w = max([len(r["name"]) for r in results] + [len("SAMPLE")])
    cw = 21  # width of per-section "raw/ratio/compressed" columns

    # Each section column shows "<uncompressed>/<compaction ratio>/<compressed>".
    def rr(raw, comp):
        return f"{human(raw)}/{ratio(raw, comp):.2f}x/{human(comp)}"

    # Per-sample table: counts, input size, raw-size/ratio of each section,
    # the same for the whole payload (TOTAL), and the whole-file in/out ratio.
    hdr = f"{'SAMPLE':<{name_w}}  {'ROWS':>9} {'KEYS':>6} {'VALUES':>9} {'INIT':>9}"
    for nm in sec_names:
        hdr += f" {nm:>{cw}}"
    hdr += f" {'TOTAL':>{cw}} {'IN/OUT':>8}"
    print(hdr)
    print("-" * len(hdr))

    for r in sorted(ok, key=lambda x: x["name"]):
        row = (f"{r['name']:<{name_w}}  {r['rows']:>9,} {r['keys']:>6,} "
               f"{r['values']:>9,} {human(r['input_file_size']):>9}")
        for nm in sec_names:
            s = sec(r, nm)
            row += f" {rr(s['raw'], s['compressed']):>{cw}}"
        row += (f" {rr(r['total_raw'], r['total_compressed']):>{cw}} "
                f"{ratio(r['input_file_size'], r['output_file_size']):>7.2f}x")
        print(row)

    n = len(ok)
    sum_rows = sum(r["rows"] for r in ok)
    sum_keys = sum(r["keys"] for r in ok)
    sum_vals = sum(r["values"] for r in ok)
    sum_init = sum(r["input_file_size"] for r in ok)
    sum_out = sum(r["output_file_size"] for r in ok)
    sum_traw = sum(r["total_raw"] for r in ok)
    sum_comp = sum(r["total_compressed"] for r in ok)
    sec_raw = {nm: sum(sec(r, nm)["raw"] for r in ok) for nm in sec_names}
    sec_comp = {nm: sum(sec(r, nm)["compressed"] for r in ok) for nm in sec_names}

    print("-" * len(hdr))
    # AVERAGE: mean raw size per section; ratio omitted (averaging ratios misleads).
    avg = (f"{'AVERAGE':<{name_w}}  {sum_rows / n:>9,.0f} {sum_keys / n:>6,.0f} "
           f"{sum_vals / n:>9,.0f} {human(sum_init / n):>9}")
    for nm in sec_names:
        avg += f" {(human(sec_raw[nm] / n) + '/-/' + human(sec_comp[nm] / n)):>{cw}}"
    avg += f" {(human(sum_traw / n) + '/-/' + human(sum_comp / n)):>{cw}} {'-':>8}"
    print(avg)
    # TOTAL: pooled raw size and byte-weighted ratio.
    tot = (f"{'TOTAL':<{name_w}}  {sum_rows:>9,} {sum_keys:>6,} {sum_vals:>9,} "
           f"{human(sum_init):>9}")
    for nm in sec_names:
        tot += f" {rr(sec_raw[nm], sec_comp[nm]):>{cw}}"
    tot += f" {rr(sum_traw, sum_comp):>{cw}} {ratio(sum_init, sum_out):>7.2f}x"
    print(tot)

    # Per-section pooled breakdown: raw, compressed, ratio, share of input.
    print("\nPer-section totals (all samples pooled):")
    print(f"  {'SECTION':<10} {'RAW':>12} {'COMP':>12} {'RATIO':>7} {'%INIT':>7}")
    for nm in sec_names:
        raw, comp = sec_raw[nm], sec_comp[nm]
        print(f"  {nm:<10} {human(raw):>12} {human(comp):>12} "
              f"{ratio(raw, comp):>6.2f}x {100.0 * raw / sum_init if sum_init else 0:>6.1f}%")

    print("\n  <section>/TOTAL columns = uncompressed size / compaction ratio / compressed size")
    print("  IN/OUT = input file size / output file size (whole-file ratio)")

    print(f"\nSamples: {n} ok" + (f", {len(bad)} failed" if bad else ""))
    for r in bad:
        print(f"  ERROR {r['name']}: {r['error']}")


def main():
    ap = argparse.ArgumentParser(
        description="Estimate compact_kv compression over multiple samples.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--samples-dir", default=DEFAULT_SAMPLES_DIR,
                    help="root directory containing sample files")
    ap.add_argument("--glob", action="append", dest="globs",
                    help="glob(s) selecting sample files by basename "
                         "(repeatable; default: '*_50k')")
    ap.add_argument("--builder", default=None,
                    help="path to the builder binary (default: ./builder next "
                         "to this script, else $PATH)")
    ap.add_argument("--zstd-level", type=int, default=4,
                    help="ZSTD level passed to the builder")
    ap.add_argument("--jobs", type=int, default=os.cpu_count(),
                    help="number of samples to process in parallel")
    ap.add_argument("--json-out", default=None,
                    help="also write the raw per-sample stats as JSON here")
    args = ap.parse_args()

    globs = args.globs or ["*_50k"]
    builder = find_builder(args.builder)

    samples = discover_samples(args.samples_dir, globs)
    if not samples:
        print(f"No samples matching {globs} under {args.samples_dir}",
              file=sys.stderr)
        return 1

    print(f"Running builder on {len(samples)} sample(s), "
          f"zstd-level={args.zstd_level}, jobs={args.jobs}...\n", file=sys.stderr)

    results = []
    with tempfile.TemporaryDirectory(prefix="ckv_est_") as tmpdir:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.jobs) as ex:
            futs = {
                ex.submit(run_one, builder, name, path, args.zstd_level, tmpdir): name
                for name, path in samples
            }
            for fut in concurrent.futures.as_completed(futs):
                results.append(fut.result())

    print_report(results)

    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(results, f, indent=2)

    return 0 if any("error" not in r for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
