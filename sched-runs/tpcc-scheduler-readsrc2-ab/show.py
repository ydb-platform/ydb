#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT = Path('/home/mfilitov/tpcc-scheduler-readsrc2-ab')
for label in sys.argv[1:]:
    folder = ROOT / label
    path = folder / 'summary.json'
    if not path.exists():
        print(label, 'no summary')
        continue
    s = json.loads(path.read_text())
    r = json.loads((folder / 'result.json').read_text())
    samples = json.loads((folder / 'cpu-samples.json').read_text())
    start = r['summary']['measure_start_ts']
    points = [(x['server']['timestamp'] - start, x['server']['cpu_seconds']) for x in samples]
    cores = []
    for t0 in range(0, 150, 15):
        window = [p for p in points if t0 <= p[0] < t0 + 15]
        if len(window) >= 2:
            cores.append(f'{(window[-1][1] - window[0][1]) / (window[-1][0] - window[0][0]):.1f}')
    no = r['transactions']['NewOrder']
    print(f"{label}: {s['tx_per_second']:.1f} tx/s, {s['server_average_cores']:.2f} cores, t={s['seconds']}s, "
          f"failed={s['failed']}, check={s.get('consistency_check_ok')}, idle={s['host']['idle_pct']:.0f}%")
    print(f"  NewOrder without queue wait p50/p95 = {no['percentiles_ms']['50']}/{no['percentiles_ms']['95']} ms")
    print(f"  cores per 15s: {' '.join(cores)}")
    c = s['counters']
    if c.get('TaskSchedulerWorkers.WorkerActivations'):
        print(f"  activations={c.get('TaskSchedulerWorkers.WorkerActivations')} empty_takes={c.get('TaskSchedulerWorkers.EmptyTakes')} "
              f"runs={c.get('TaskSchedulerPoC.Runs')} no_progress={c.get('TaskSchedulerPoC.NoObservedProgressRuns')}")
