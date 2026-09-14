#!/usr/bin/env python3
"""Compare one identical layout harness built against old and widened production code."""

import argparse
import datetime
import hashlib
import json
import os
import pathlib
import statistics
import subprocess


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def capture(cmd):
    result = subprocess.run(cmd, text=True, capture_output=True, check=False)
    return dict(command=cmd, returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=pathlib.Path)
    parser.add_argument("current", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--baseline-commit", default="46846f03113d4b731e6507780f7b70a52c6d5425")
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=1 << 20)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    args = parser.parse_args()
    if args.repeats < 10 or args.iterations < 256:
        parser.error("at least ten independent runs and 256 iterations are required")
    args.baseline, args.current, args.output = args.baseline.resolve(), args.current.resolve(), args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    raw = args.output / "raw"
    raw.mkdir()
    for directory in ["plots", "load-actor-configs"]:
        (args.output / directory).mkdir()
    root = pathlib.Path(__file__).resolve().parents[5]
    metadata = dict(start=now(), command=os.sys.argv, baseline_commit=args.baseline_commit,
                    current_git=capture(["git", "-C", str(root), "rev-parse", "HEAD"]),
                    current_status=capture(["git", "-C", str(root), "status", "--short"]),
                    lscpu=capture(["lscpu", "-J"]), cpu=args.cpu, repeats=args.repeats,
                    iterations=args.iterations, compiler_flags="./ya make --build relwithdebinfo ydb/core/blobstorage/groupinfo/benchmark",
                    binaries={label:dict(path=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
                              for label,binary in [("baseline",args.baseline),("current",args.current)]},
                    source_sha256={str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest()
                                   for path in [root/"ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h",
                                                root/"ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.cpp",
                                                pathlib.Path(__file__).with_name("layout_bench.cpp")]})
    (args.output/"run-metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    records=[]
    variants=[("old42",args.baseline,"42"),("new42",args.current,"42"),("new82",args.current,"82")]
    for repeat in range(args.repeats):
        order=variants[repeat%3:]+variants[:repeat%3]
        if repeat%2:order=list(reversed(order))
        for label,binary,species in order:
            cmd=["taskset","-c",str(args.cpu),str(binary),species,str(args.iterations)]
            start=now()
            result=capture(cmd)
            record=dict(label=label,repeat=repeat,start=start,end=now(),**result)
            if result["returncode"]==0:
                record["result"]=json.loads(result["stdout"])
            (raw/f"{repeat:02d}-{label}.json").write_text(json.dumps(record,indent=2)+"\n")
            if "result" not in record:
                raise RuntimeError(f"failed {repeat}/{label}: {result['stderr']}")
            records.append(record)
        print(f"completed repeat {repeat+1}/{args.repeats}",flush=True)
    rows=[]
    for operation in [row["operation"] for row in records[0]["result"]["measurements"]]:
        row=dict(operation=operation)
        for label,_,_ in variants:
            values=[next(value["ns_per_operation"] for value in record["result"]["measurements"] if value["operation"]==operation)
                    for record in records if record["label"]==label]
            median=statistics.median(values)
            row[label]=dict(ns_per_operation=median,mad_ns=statistics.median(abs(value-median) for value in values))
        ratios=[]
        for repeat in range(args.repeats):
            pair={record["label"]:next(value["ns_per_operation"] for value in record["result"]["measurements"] if value["operation"]==operation)
                  for record in records if record["repeat"]==repeat}
            ratios.append(pair["new42"]/pair["old42"])
        row["paired_new42_over_old42_cost"]=statistics.median(ratios)
        rows.append(row)
    (args.output/"samples.json").write_text(json.dumps(records,indent=2)+"\n")
    (args.output/"summary.json").write_text(json.dumps(rows,indent=2)+"\n")
    with (args.output/"summary.md").open("w") as output:
        output.write("# Layout footprint and hot operations\n\nSame harness, exact production methods, 256 deterministic cases, ten independent processes per variant. Mutation rows include a layout copy; CopyControl reports that cost separately. No outliers removed. CPU affinity and first-touch allocation are used.\n\n")
        output.write("| Variant | sizeof layout | alignof layout | sizeof part vector | sizeof persistent ingress |\n|---|---:|---:|---:|---:|\n")
        for label,_,_ in variants:
            result=next(record["result"] for record in records if record["label"]==label)
            output.write(f"| {label} | {result['sizeof_layout']} | {result['alignof_layout']} | {result['sizeof_parts_vector']} | {result['sizeof_ingress']} |\n")
        output.write("\n| Operation | Old42 ns | New42 ns | New82 ns | New42 / Old42 cost |\n|---|---:|---:|---:|---:|\n")
        for row in rows:
            output.write(f"| {row['operation']} | {row['old42']['ns_per_operation']:.2f} | {row['new42']['ns_per_operation']:.2f} | {row['new82']['ns_per_operation']:.2f} | {row['paired_new42_over_old42_cost']:.3f} |\n")
        output.write("\nRow5 primitive corpus includes disk bits3 and4, spanning cells63/64 with stride12. Matching corpus uses valid main/handoff placements and is checked against an independent augmenting-path oracle outside timing. New82 has more parts/disks; it is a separate geometry, not a pure representation comparison.\n\nLayout heap ownership is assessed from fixed inline storage and local stack-vector capacity in the production source; no dynamic allocation-counter result is claimed. Timer ticks are not CPU cycles. These are hot microbenchmarks, not DSProxy throughput or an ABI-size guarantee.\n")
    metadata["end"]=now()
    (args.output/"run-metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    (args.output/"experiment-log.md").write_text(f"# Layout comparison\n\nStart: {metadata['start']}\n\nEnd: {metadata['end']}\n\nBaseline production source: {args.baseline_commit}. Identical harness was added uncommitted to a detached baseline worktree; no old layout algorithm was copied into new production sources. Main/handoff matching and boundary primitives are separate corpora. Correctness checks precede and follow timing. All raw process output is retained. No cluster was used.\n")


if __name__=="__main__":
    main()
