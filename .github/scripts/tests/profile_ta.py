"""Profile only the ya runner during the first wave of result-aggregation tasks."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


evlog, output, private, stop_file = map(Path, sys.argv[1:])
output.mkdir(parents=True, exist_ok=True)
private.mkdir(parents=True, exist_ok=True)
stop_requested = False


def stop(signum, frame):
    global stop_requested
    stop_requested = True


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def stopped():
    return stop_requested or stop_file.exists()


def find_runner():
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / 'cmdline').read_bytes().split(b'\0')
            # Match this attempt, not build preparation, test_tool or a ydbd child.
            if Path(os.fsdecode(args[0])).name == 'ya-bin' and b'make' in args:
                if os.fsencode(str(evlog)) in args:
                    return int(proc.name)
        except (OSError, IndexError):
            pass
    return None


def wait_for_ta():
    deadline = time.monotonic() + 3 * 3600
    while not evlog.exists() and not stopped() and time.monotonic() < deadline:
        time.sleep(1)
    if not evlog.exists():
        return None
    active = set()
    with evlog.open() as source:
        while not stopped() and time.monotonic() < deadline:
            position = source.tell()
            line = source.readline()
            if not line.endswith('\n'):
                source.seek(position)
                time.sleep(.25)
                continue
            event = json.loads(line)
            if event.get('event') not in {'node-started', 'node-finished'}:
                continue
            name = event.get('value', {}).get('name', '')
            if not name.startswith('Run(') or 'results_accumulator.log' not in name:
                continue
            if event['event'] == 'node-started':
                active.add(name)
            elif event['event'] == 'node-finished':
                active.discard(name)
            # Ignore an isolated early TA; catch the beginning of the actual wave.
            if len(active) >= 8:
                return {'trigger_ts': event['timestamp'], 'active_ta': len(active)}
    return None


def snapshot_threads(pid, target):
    for task in Path('/proc/{}/task'.format(pid)).iterdir():
        try:
            row = {'ts': time.time(), 'tid': int(task.name)}
            for name in ['comm', 'stat', 'wchan', 'stack']:
                try:
                    row[name] = (task / name).read_text()
                except OSError:
                    row[name] = None
            target.write(json.dumps(row) + '\n')
        except OSError:
            pass
    target.flush()


def main():
    metadata = wait_for_ta()
    if metadata is None:
        (output / 'status.json').write_text(json.dumps({'status': 'no_ta_wave_before_stop'}))
        return
    pid = find_runner()
    if pid is None:
        raise RuntimeError('TA wave detected, but the ya make PID was not found')
    metadata.update(pid=pid, capture_started=time.time(), duration_limit_seconds=180)
    (output / 'status.json').write_text(json.dumps(metadata, indent=2))
    perf = os.environ['TA_PERF']
    commands = {
        'perf': [perf, 'record', '-e', 'cpu-clock', '-F', '49', '--no-inherit',
                 '--call-graph', 'dwarf,8192', '-p', str(pid),
                 '-o', str(private / 'perf.data'), '--', 'sleep', '180'],
    }
    children = []
    try:
        for name, command in commands.items():
            with (output / (name + '.log')).open('w') as log:
                child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                         start_new_session=True)
            children.append((name, child))
        deadline = time.monotonic() + 180
        with (output / 'threads.jsonl').open('w') as snapshots:
            while not stopped() and time.monotonic() < deadline and Path('/proc/{}'.format(pid)).exists():
                snapshot_threads(pid, snapshots)
                time.sleep(2)
    finally:
        for name, child in children:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGINT)
        for name, child in children:
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            metadata[name + '_exit_code'] = child.returncode
        metadata['capture_finished'] = time.time()
        # Raw DWARF samples contain stack memory: keep them out of public artifacts.
        # Publish symbolized reports only, without locals, environment or raw stacks.
        for name, options in [('perf-flat.txt', ['--call-graph', 'none']),
                              ('perf-stacks.txt', [])]:
            with (output / name).open('w') as report:
                result = subprocess.run([perf, 'report', '--stdio', '--children', '--sort', 'symbol',
                                         '--percent-limit', '0.5', '-i', str(private / 'perf.data')] + options,
                                        stdout=report, stderr=subprocess.STDOUT, timeout=90)
                metadata[name + '_exit_code'] = result.returncode
        # SIGINT is the normal way to flush perf when the attempt finishes early.
        reports_ok = all(metadata.get(name + '_exit_code') == 0
                         for name in ['perf-flat.txt', 'perf-stacks.txt'])
        metadata['status'] = ('complete' if reports_ok and metadata.get('perf_exit_code') in (0, -signal.SIGINT)
                              else 'partial')
        (output / 'status.json').write_text(json.dumps(metadata, indent=2))


if __name__ == '__main__':
    main()
