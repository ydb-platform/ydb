"""Tiny real subprocesses scheduled by ya make; no YDB compilation."""

import argparse
import json
import os
from pathlib import Path
import time

p = argparse.ArgumentParser()
p.add_argument("kind", choices=["archive", "step"])
p.add_argument("--output", type=Path, required=True)
p.add_argument("--run-dir", type=Path, required=True)
p.add_argument("--archive", type=Path)
p.add_argument("--index", type=int, default=0)
p.add_argument("--delay", type=float, default=0.05)
args = p.parse_args()
args.output.parent.mkdir(parents=True, exist_ok=True)
if args.kind == "archive":
    os.link(args.archive, args.output)
else:
    started = time.monotonic()
    if args.index == 0:
        deadline = started + 60
        while not (args.run_dir / "extracting").exists():
            if time.monotonic() > deadline:
                raise TimeoutError("Archive extraction did not start")
            time.sleep(0.002)
    time.sleep(args.delay)
    row = {
        "index": args.index,
        "started": started,
        "finished": time.monotonic(),
        "pid": os.getpid(),
    }
    # Independent evidence written by the child, even when ya's Python is blocked.
    text = json.dumps(row)
    (args.run_dir / ("child-%03d.json" % args.index)).write_text(text)
    args.output.write_text(text)
