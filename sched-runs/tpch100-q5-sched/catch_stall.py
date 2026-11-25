#!/usr/bin/env python3
"""Run Q5 until the process stalls, then record which actors still receive events."""
import collections
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).parent))
import run as q5  # noqa: E402

ROOT = q5.ROOT


def counters(ports):
    events = collections.Counter()
    elapsed = collections.Counter()
    other = {}
    for port in ports:
        with urlopen(f'http://127.0.0.1:{port}/counters/json', timeout=30) as response:
            data = json.load(response)
        for sensor in data.get('sensors', []):
            labels = sensor.get('labels', {})
            value = sensor.get('value')
            if not isinstance(value, (int, float)):
                continue
            name = labels.get('sensor', labels.get('name', ''))
            if name == 'ReceivedEventsByActivity':
                events[labels.get('activity', '?')] += value
            elif name == 'ElapsedMicrosecByActivity':
                elapsed[labels.get('activity', '?')] += value
            elif any(word in name for word in ('Spill', 'Memory', 'Quota', 'Waiting', 'InFlight')) and 'execpool' not in labels:
                key = '/'.join(f'{k}={v}' for k, v in sorted(labels.items()) if k != 'sensor') + '/' + name
                other[key] = other.get(key, 0) + value
    return events, elapsed, other


def main(label, mode):
    folder = ROOT / label
    binary = ROOT / 'kqprun'
    enabled = mode == 'on'
    if folder.exists():
        import shutil
        shutil.rmtree(folder)
    folder.mkdir()
    for name in ('sync', 'spill'):
        (folder / name).mkdir()
    cmd = [str(binary), '-N', str(q5.NODES), '--threads', '1', '-c', str(q5.config(folder, enabled, 4, 2)),
           '-s', str(ROOT / 'scheme.yql'), '-p', str(ROOT / 'q5_shuffle.yql'), '-C', 'query', '--stats-mode', 'basic',
           '--result-file', str(folder / 'result.json'), '-M', '--log-file', str(folder / 'server.log'), '--log-default', 'warn']
    env = dict(os.environ, PORT_SYNC_PATH=str(folder / 'sync'), **(q5.FAST_ENV if enabled else {}))
    log_path = folder / 'kqprun.log'
    with log_path.open('w') as output:
        process = subprocess.Popen(cmd, cwd=q5.KQP, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    start = time.monotonic()
    history = []
    report = None
    try:
        while process.poll() is None and time.monotonic() - start < 900:
            history.append(q5.sample(process.pid))
            time.sleep(1)
            if len(history) < 25 or report is not None:
                continue
            a, b = history[-11], history[-1]
            cores = (b['cpu'] - a['cpu']) / (b['t'] - a['t'])
            if cores < 4.0:
                log = q5.ANSI.sub('', log_path.read_text(errors='replace'))
                ports = sorted({int(p) for p in q5.PORT.findall(log)})
                e1, t1, o1 = counters(ports)
                time.sleep(10)
                e2, t2, o2 = counters(ports)
                report = dict(at_seconds=time.monotonic() - start, cores_before=cores,
                              events={k: e2[k] - e1[k] for k in e2 if e2[k] - e1[k]},
                              elapsed_us={k: t2[k] - t1[k] for k in t2 if t2[k] - t1[k]},
                              gauges=o2)
                q5.save(folder / 'stall.json', report)
                print(f'stall at {report["at_seconds"]:.0f}s, cores {cores:.1f}', flush=True)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait()
    q5.save(folder / 'samples.json', history)
    log = q5.ANSI.sub('', log_path.read_text(errors='replace'))
    print(label, 'durations', q5.DURATION.findall(log), 'stalled', report is not None, flush=True)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
