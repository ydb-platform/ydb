import argparse
import hashlib
import json
from pathlib import Path
import shutil
import statistics

p = argparse.ArgumentParser()
p.add_argument("--root", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--mib", type=int, default=1024)
p.add_argument("cases", nargs="+")
args = p.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
rows = []
for name in args.cases:
    directory = args.root / name
    probe = json.loads((directory / "probe.json").read_text())
    children = [
        json.loads(f.read_text()) for f in sorted(directory.glob("child-*.json"))
    ]
    assert len(children) == 40
    assert len(probe["calls"]) == 1
    call = probe["calls"][0]
    a, b = call["start"], call["end"]
    gaps = [
        y - x
        for x, y in zip(probe["heartbeat"], probe["heartbeat"][1:])
        if x < b and y > a
    ]
    # Save only worker events; the full evlog includes environment metadata.
    events = []
    for line in (directory / "evlog.jsonl").open():
        event = json.loads(line)
        if event.get("namespace") == "worker_threads":
            events.append(event)
    nodes = [
        e["value"]
        for e in events
        if e.get("event") == "node-finished" and "gil-" in e["value"].get("name", "")
    ]
    graph_seconds = max(n["time"][1] for n in nodes) - min(n["time"][0] for n in nodes)
    output = directory / "out/gil_probe/payload/large.log"
    assert output.stat().st_size == args.mib * 1024**2
    rows.append(
        {
            "case": name,
            "mode": probe["mode"],
            "extraction_seconds": b - a,
            "heartbeat_gap_seconds": max(gaps),
            "dependency_start_delay_seconds": children[1]["started"]
            - children[0]["finished"],
            "children_started_during_extraction": sum(
                a < x["started"] < b for x in children
            ),
            "graph_seconds": graph_seconds,
            "cli_seconds": probe["wall_seconds"],
            "probe": probe,
            "children": children,
        }
    )
    dest = args.output / name
    dest.mkdir(exist_ok=True)
    shutil.copyfile(directory / "targeted-stacks.txt", dest / "targeted-stacks.txt")
    shutil.copyfile(directory / "graph.json", dest / "graph.json")
    (dest / "worker-events.jsonl").write_text(
        "".join(json.dumps(e) + "\n" for e in events)
    )

verification = {}
expected = (args.root / "payload.tar.zstd.sha256").read_text().strip()
for mode in ("original", "release"):
    row = next(r for r in rows if r["mode"] == mode)
    path = args.root / row["case"] / "out/gil_probe/payload/large.log"
    # libarchive's writestream creates an entry with mode 000 by default.
    # Change permissions only after all measurements, to verify payload bytes.
    path.chmod(0o600)
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    digest = hasher.hexdigest()
    assert digest == expected
    verification[mode] = digest
metrics = [
    "extraction_seconds",
    "heartbeat_gap_seconds",
    "dependency_start_delay_seconds",
    "children_started_during_extraction",
    "graph_seconds",
    "cli_seconds",
]
medians = {
    mode: {
        k: statistics.median(r[k] for r in rows if r["mode"] == mode) for k in metrics
    }
    for mode in ("original", "release")
}
report = {
    "input_bytes": args.mib * 1024**2,
    "medians": medians,
    "output_sha256": verification,
    "runs": rows,
}
(args.output / "measurements.json").write_text(json.dumps(report, indent=2))
print(json.dumps({"medians": medians, "output_sha256": verification}, indent=2))
