#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT = Path('/home/mfilitov/tpch100-q5-sched')
STEP = 5
for label in sys.argv[1:]:
    samples = [s for s in json.loads((ROOT / label / 'samples.json').read_text()) if s['cpu'] is not None]
    print(f'== {label}: t, process cores, host user/sys/idle/iowait/steal %, disk read/write MB/s')
    start = samples[0]['t']
    i = 0
    while i < len(samples) - 1:
        j = i
        while j < len(samples) - 1 and samples[j]['t'] - samples[i]['t'] < STEP:
            j += 1
        a, b = samples[i], samples[j]
        dt = b['t'] - a['t']
        total = b['total'] - a['total'] or 1
        pct = {k: 100 * (b[k] - a[k]) / total for k in ('user', 'system', 'idle', 'iowait', 'steal')}
        print(f"{a['t'] - start:5.0f}s {(b['cpu'] - a['cpu']) / dt:5.1f} cores  "
              f"{pct['user']:3.0f}/{pct['system']:3.0f}/{pct['idle']:3.0f}/{pct['iowait']:3.0f}/{pct['steal']:3.0f}  "
              f"{(b['read'] - a['read']) / dt / 1e6:6.0f}/{(b['write'] - a['write']) / dt / 1e6:6.0f}")
        i = j
