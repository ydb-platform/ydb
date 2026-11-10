#!/usr/bin/env python3
"""Temporary try_1 kernel profiling for the no-mlock experiment."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    out, monitor, perf = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def stopped():
        return stopping or (out / 'stop').exists()

    def snapshot(prefix):
        for name in ('stat', 'vmstat', 'meminfo', 'pressure/cpu', 'pressure/io', 'pressure/memory'):
            try:
                (out / (prefix + '-' + name.replace('/', '-') + '.txt')).write_text(
                    Path('/proc', name).read_text())
            except OSError as error:
                print(name, error, flush=True)
        with (out / (prefix + '-processes.txt')).open('w') as target:
            subprocess.run(['ps', '-eo', 'pid,ppid,nlwp,stat,comm'], stdout=target, check=False)

    with (out / 'kallsyms.txt').open('w') as target:
        subprocess.run(['sudo', '-n', 'cat', '/proc/kallsyms'], stdout=target, check=True)
    with (out / 'host.txt').open('w') as target:
        subprocess.run(['uname', '-a'], stdout=target, check=True)
        subprocess.run(['lscpu'], stdout=target, check=False)

    deadline = time.monotonic() + 6 * 3600
    origin = None
    while not stopped() and time.monotonic() < deadline:
        try:
            with monitor.open() as source:
                origin = json.loads(source.readline())['ts']
            break
        except (OSError, ValueError, KeyError):
            time.sleep(1)
    if origin is None:
        return

    for minute in (10, 35, 65):
        while not stopped() and time.time() < origin + minute * 60:
            time.sleep(1)
        if stopped():
            break
        name = 'minute-{}'.format(minute)
        data = out / (name + '.data')
        snapshot(name + '-before')
        info = {'scheduled_minute': minute, 'start_epoch': time.time(), 'frequency_hz': 49}
        print(name, 'start', info['start_epoch'], flush=True)
        with (out / (name + '-record.log')).open('w') as target:
            proc = subprocess.Popen([
                'sudo', '-n', 'timeout', '-s', 'INT', '-k', '5', '110',
                perf, 'record', '-a', '-e', 'cpu-clock:k', '-F', '49',
                '-g', '--kernel-callchains', '--no-buildid', '--no-buildid-cache',
                '-m', '128', '-o', str(data), '--', 'sleep', '90',
            ], stdout=target, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                while proc.poll() is None and not stopped():
                    time.sleep(1)
            finally:
                if proc.poll() is None:
                    # Stop the root-owned recorder too; do not capture postprocessing/retries.
                    subprocess.run(['sudo', '-n', '/bin/kill', '-INT', '--', '-' + str(proc.pid)],
                                   check=False, timeout=10)
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        subprocess.run(['sudo', '-n', '/bin/kill', '-KILL', '--', '-' + str(proc.pid)],
                                       check=False, timeout=10)
                        proc.wait(timeout=10)
                if data.exists():
                    subprocess.run(['sudo', '-n', 'chown', '{}:{}'.format(os.getuid(), os.getgid()), str(data)],
                                   check=False, timeout=10)
        info.update(end_epoch=time.time(), returncode=proc.returncode, interrupted=stopped())
        (out / (name + '.json')).write_text(json.dumps(info, indent=2))
        snapshot(name + '-after')
        print(name, 'finished', info, flush=True)
        if proc.returncode != 0:
            break


if __name__ == '__main__':
    main()
