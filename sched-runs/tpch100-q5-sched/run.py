#!/usr/bin/env python3
"""TPC-H SF100 Q5 on 16 kqprun nodes: scheduling off vs on, with actor and pool time."""
import collections
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

ROOT = Path('/home/mfilitov/tpch100-q5-sched')
DATA = Path('/home/mfilitov/tpch100-q5-ab')
REPO = Path('/home/mfilitov/ydb-pr56258-local-consumer')
KQP = REPO / 'ydb/tests/tools/kqprun'
NODES = 16
TIMEOUT = 900
DURATION = re.compile(r'(?:request finished\. (?:Total duration|Estimated duration|Duration): |Request #\d+ completed\..*?spend time: )([0-9.]+)s')
ANSI = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')
PORT = re.compile(r'Monitoring port[^:\n]*:\s*(\d+)')
FAST_ENV = dict(YDB_DQ_TASK_PREFER_CONSUMERS='0', YDB_DQ_TASK_PREFER_READY_QUERIES='0', YDB_DQ_TASK_TAIL_WAKE='1',
                YDB_DQ_TASK_FIFO='1', YDB_DQ_TASK_CHEAP_RUN='1', YDB_DQ_TASK_WAIT_FIRST_PIPELINE='1',
                YDB_DQ_TASK_FINISH_ONCE='1', YDB_DQ_TASK_WAIT_FIRST_INPUT='1', YDB_DQ_TASK_REFRESH_INLINE='1')


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temp.replace(path)


def result_rows(path):
    text = path.read_text().lstrip()
    rows = []
    decoder = json.JSONDecoder()
    while text:
        items, n = decoder.raw_decode(text)
        rows.extend(items if isinstance(items, list) else [items])
        text = text[n:].lstrip()
    return rows


def comparable(a, b):
    a = {r['n_name']: float(r['revenue']) for r in a}
    b = {r['n_name']: float(r['revenue']) for r in b}
    return len(a) == 5 and a.keys() == b.keys() and all(math.isclose(a[k], b[k], rel_tol=1e-9) for k in a)


def config(folder, enabled, shards, inflight):
    base = (KQP / 'configuration/app_config.conf').read_text()
    spilling = os.environ.get('Q5_SPILL', '1') != '0'
    for old, new in [] if not spilling else [
        ('EnableQueryServiceSpilling: false', 'EnableQueryServiceSpilling: true'),
        ('EnableSpillingNodes: "None"', 'EnableSpillingNodes: "All"'),
        ('EnableSpillingInHashJoinShuffleConnections: false', 'EnableSpillingInHashJoinShuffleConnections: true'),
    ] + ([] if os.environ.get('Q5_MEM') == 'default' else [
        ('QueryMemoryLimit: 64424509440', 'QueryMemoryLimit: 34359738368\n    SpillingPercent: 10'),
    ]):
        assert base.count(old) == 1, old
        base = base.replace(old, new)
    path = folder / 'config.conf'
    path.write_text(base.replace('TableServiceConfig {', f'''TableServiceConfig {{
  DqChannelVersion: 2
  SpillingServiceConfig {{
    LocalFileConfig {{ Enable: {str(spilling).lower()} Root: "{folder / 'spill'}" MaxTotalSize: 3221225472
      IoThreadPool {{ WorkersCount: 1 }} }}
  }}
  DqChannelConfig {{
    EnableTaskScheduling: {str(enabled).lower()}
    TaskSchedulerPreferLocalConsumer: true
    TaskSchedulerUseWorkerPool: true
    TaskSchedulerObserveOnly: false
    TaskSchedulerShards: {shards}
    TaskSchedulerMaxInFlightPerShard: {inflight}
    TaskSchedulerMaxRowsPerRun: 0
    TaskSchedulerWorkerSliceCpuUs: {os.environ.get('Q5_SLICE_US', '1000')}
    TaskSchedulerWorkerActivationUs: {os.environ.get('Q5_ACTIVATION_US', '2000')}
    EnableTaskExecutionTrace: false
  }}''', 1))
    return path


def scrape(ports):
    activity = collections.Counter()
    pools = collections.Counter()
    sched = collections.Counter()
    for port in ports:
        with urlopen(f'http://127.0.0.1:{port}/counters/json', timeout=30) as response:
            data = json.load(response)
        for sensor in data.get('sensors', []):
            labels = sensor.get('labels', {})
            value = sensor.get('value')
            if not isinstance(value, (int, float)):
                continue
            name = labels.get('sensor', labels.get('name', ''))
            if name == 'ElapsedMicrosecByActivity':
                activity[labels.get('activity', '?')] += value
            elif 'execpool' in labels and name in ('ElapsedMicrosec', 'ParkedMicrosec', 'CpuMicrosec'):
                pools[labels['execpool'] + '.' + name] += value
            elif labels.get('component') in ('TaskSchedulerWorkers', 'TaskSchedulerPoC'):
                sched[labels['component'] + '.' + name] += value
    return dict(activity=dict(activity), pools=dict(pools), scheduler=dict(sched))


def sample(pid):
    host = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    io = {}
    try:
        for row in Path(f'/proc/{pid}/io').read_text().splitlines():
            key, value = row.split(':')
            io[key] = int(value)
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        cpu = (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')
        rss = int(stat[21]) * os.sysconf('SC_PAGE_SIZE')
    except (FileNotFoundError, ProcessLookupError):
        cpu = rss = None
    return dict(t=time.monotonic(), user=host[0] + host[1], system=host[2], idle=host[3], iowait=host[4],
                steal=host[7], total=sum(host), cpu=cpu, rss=rss, read=io.get('read_bytes'), write=io.get('write_bytes'))


def run(label, enabled, binary, shards=4, inflight=2, threads=1):
    folder = ROOT / label
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir()
    for name in ('sync', 'spill'):
        (folder / name).mkdir()
    cmd = [str(binary), '-N', str(NODES), '--threads', str(threads), '-c', str(config(folder, enabled, shards, inflight)),
           '-s', str(ROOT / 'scheme.yql'), '-p', str(ROOT / 'q5_shuffle.yql'), '--stats-mode', 'basic',
           '--result-file', str(folder / 'result.json'), '-M', '--hold']
    concurrency = int(os.environ.get('Q5_CONCURRENCY', '1'))
    if concurrency > 1:
        cmd += ['-C', 'async', '--loop-count', str(concurrency), '--inflight-limit', str(concurrency)]
    else:
        cmd += ['-C', 'query']
    if os.environ.get('Q5_LOG'):
        cmd += ['--log-file', str(folder / 'server.log'), '--log-default', 'warn']
        cmd += [arg for item in os.environ['Q5_LOG'].split(',') if item for arg in ('--log', item)]
    drop = set(filter(None, os.environ.get('Q5_ENV_DROP', '').split(',')))
    fast = {k: v for k, v in FAST_ENV.items() if k not in drop}
    env = dict(os.environ, PORT_SYNC_PATH=str(folder / 'sync'), **(fast if enabled else {}))
    save(folder / 'command.json', dict(argv=cmd, binary_inode=binary.stat().st_ino, enabled=enabled,
                                       shards=shards, inflight=inflight, threads=threads))
    cpu0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    counters = None
    error = None
    log_path = folder / 'kqprun.log'
    with log_path.open('w') as output:
        process = subprocess.Popen(cmd, cwd=KQP, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    samples = []
    perf = None
    perf_window = os.environ.get('Q5_PERF')
    try:
        while True:
            samples.append(sample(process.pid))
            if os.environ.get('Q5_PERFSTAT') and perf is None:
                perf = subprocess.Popen(['perf', 'stat', '-e', os.environ['Q5_PERFSTAT'], '-p', str(process.pid),
                                         '-o', str(folder / 'perfstat.txt')],
                                        stdout=subprocess.DEVNULL, stderr=(folder / 'perf.log').open('w'))
            if perf_window and perf is None and time.monotonic() - start > 15:
                perf = subprocess.Popen(['perf', 'record', '-F', '499', '-g', '-p', str(process.pid),
                                         '-o', str(folder / 'perf.data'), '--', 'sleep', perf_window],
                                        stdout=subprocess.DEVNULL, stderr=(folder / 'perf.log').open('w'))
            if process.poll() is not None:
                error = f'kqprun exited with {process.returncode}'
                break
            log = ANSI.sub('', log_path.read_text(errors='replace'))
            if 'Initialization finished' in log and len(DURATION.findall(log)) >= concurrency:
                ports = sorted({int(p) for p in PORT.findall(log)})
                counters = scrape(ports)
                break
            if time.monotonic() - start > TIMEOUT:
                error = 'query timeout'
                break
            meminfo = dict(row.split(':', 1) for row in Path('/proc/meminfo').read_text().splitlines())
            if int(meminfo['MemAvailable'].split()[0]) < 4 * 1024 ** 2:
                error = 'free memory guard'
                break
            if shutil.disk_usage(ROOT).free < 8 * 1024 ** 3:
                error = 'free disk guard'
                break
            time.sleep(0.5)
    finally:
        if perf is not None and os.environ.get('Q5_PERFSTAT'):
            perf.send_signal(signal.SIGINT)
        if perf is not None:
            perf.wait()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    cpu1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    save(folder / 'samples.json', samples)
    shutil.rmtree(folder / 'spill', ignore_errors=True)
    log = ANSI.sub('', log_path.read_text(errors='replace'))
    durations = [float(v) for v in DURATION.findall(log)]
    summary = dict(label=label, enabled=enabled, shards=shards, inflight=inflight, threads=threads, error=error,
                   cpu_seconds=cpu1.ru_utime - cpu0.ru_utime + cpu1.ru_stime - cpu0.ru_stime,
                   user_seconds=cpu1.ru_utime - cpu0.ru_utime, system_seconds=cpu1.ru_stime - cpu0.ru_stime,
                   voluntary_switches=cpu1.ru_nvcsw - cpu0.ru_nvcsw, involuntary_switches=cpu1.ru_nivcsw - cpu0.ru_nivcsw,
                   process_seconds=time.monotonic() - start, counters=counters)
    summary['peak_rss_gb'] = max((s.get('rss') or 0 for s in samples), default=0) / 1024 ** 3
    if concurrency > 1:
        summary.update(concurrency=concurrency, durations=durations,
                       wall_seconds=summary['process_seconds'], result_ok=None)
        if len(durations) != concurrency:
            summary['error'] = error or f'{len(durations)} of {concurrency} durations'
    elif not error and len(durations) == 1 and (folder / 'result.json').is_file():
        rows = result_rows(folder / 'result.json')
        summary.update(query_seconds=durations[0], result_ok=comparable(json.loads((DATA / 'reference.json').read_text()), rows))
    else:
        summary['error'] = error or 'no duration or result'
    save(folder / 'summary.json', summary)
    print(json.dumps({k: v for k, v in summary.items() if k != 'counters'}), flush=True)
    return summary


def main():
    binary = ROOT / 'kqprun'
    current = (KQP / 'kqprun').resolve(strict=True)
    if binary.exists() and binary.stat().st_ino != current.stat().st_ino:
        binary.unlink()
    if not binary.exists():
        os.link(current, binary)
    (ROOT / 'scheme.yql').write_text((DATA / 'scheme.yql').read_text())
    (ROOT / 'q5_shuffle.yql').write_text((DATA / 'q5_shuffle.yql').read_text())
    plan = [arg.split(':') for arg in sys.argv[1:]] or [['off-1', 'off'], ['on-1', 'on'], ['on-2', 'on'], ['off-2', 'off']]
    saved = ROOT / 'runs.json'
    runs = json.loads(saved.read_text()) if saved.exists() else []
    for item in plan:
        label, mode = item[0], item[1]
        extra = dict(zip(('shards', 'inflight', 'threads'), map(int, item[2:])))
        if any(r['label'] == label for r in runs):
            continue
        runs.append(run(label, mode == 'on', binary, **extra))
        save(saved, runs)


if __name__ == '__main__':
    main()
