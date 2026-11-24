#!/usr/bin/env python3
"""All 22 TPC-H SF10 queries at once, scheduler off vs on."""
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path('/home/mfilitov/tpch10-all-sched')
SUITE = Path('/home/mfilitov/tpch10-suite-ab')
REPO = Path('/home/mfilitov/ydb-pr56258-local-consumer')
KQP = REPO / 'ydb/tests/tools/kqprun'
NODES = int(os.environ.get('KQPRUN_NODES', '16'))
QUERIES = 22
TIMEOUT = int(os.environ.get('KQPRUN_TIMEOUT', '1200'))
ANSI = re.compile(r'\x1b\[[0-9;]*[a-zA-Z]')
DONE = re.compile(r'Request #(\d+) completed\..*?spend time: ([0-9.]+)s')
FAIL = re.compile(r'Request #(\d+) failed')
FAST_ENV = dict(YDB_DQ_TASK_PREFER_CONSUMERS='0', YDB_DQ_TASK_PREFER_READY_QUERIES='0', YDB_DQ_TASK_TAIL_WAKE='1',
                YDB_DQ_TASK_FIFO='1', YDB_DQ_TASK_CHEAP_RUN='1', YDB_DQ_TASK_WAIT_FIRST_PIPELINE='1',
                YDB_DQ_TASK_FINISH_ONCE='1', YDB_DQ_TASK_WAIT_FIRST_INPUT='1', YDB_DQ_TASK_REFRESH_INLINE='1',
                YDB_DQ_TASK_S3_SOURCES='1')


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


def config(folder, enabled, shards, inflight):
    base = (KQP / 'configuration/app_config.conf').read_text()
    for old, new in [
        ('EnableQueryServiceSpilling: false', 'EnableQueryServiceSpilling: true'),
        ('EnableSpillingNodes: "None"', 'EnableSpillingNodes: "All"'),
        ('EnableSpillingInHashJoinShuffleConnections: false', 'EnableSpillingInHashJoinShuffleConnections: true'),
    ]:
        assert base.count(old) == 1, old
        base = base.replace(old, new)
    path = folder / 'config.conf'
    path.write_text(base.replace('TableServiceConfig {', f'''TableServiceConfig {{
  DqChannelVersion: 2
  SpillingServiceConfig {{
    LocalFileConfig {{ Enable: true Root: "{folder / 'spill'}" MaxTotalSize: 17179869184
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
    TaskSchedulerWorkerSliceCpuUs: 1000
    TaskSchedulerWorkerActivationUs: 2000
    EnableTaskExecutionTrace: false
  }}''', 1))
    return path


def sample(pid):
    host = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    try:
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        rss = int(stat[21]) * os.sysconf('SC_PAGE_SIZE')
    except (FileNotFoundError, ProcessLookupError):
        rss = None
    return dict(t=time.monotonic(), user=host[0] + host[1], system=host[2], idle=host[3], iowait=host[4],
                steal=host[7], total=sum(host), rss=rss)


def run(label, enabled, binary, shards=1, inflight=1, threads=1, nodes=NODES):
    folder = ROOT / label
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir()
    (folder / 'sync').mkdir()
    (folder / 'spill').mkdir()
    cmd = [str(binary), '-N', str(nodes), '--threads', str(threads), '-c', str(config(folder, enabled, shards, inflight)),
           '-s', str(SUITE / 'scheme.yql'), '-u', str(SUITE / 'libmath_udf.so'),
           '--stats-mode', 'basic', '-M', '--hold', '-C', 'async', '--inflight-limit', str(QUERIES)]
    for number in range(1, QUERIES + 1):
        cmd += ['-p', str(SUITE / 'queries' / f'q{number:02}.yql')]
    env = dict(os.environ, PORT_SYNC_PATH=str(folder / 'sync'), **(FAST_ENV if enabled else {}))
    save(folder / 'command.json', dict(argv=cmd, enabled=enabled, shards=shards, inflight=inflight, nodes=nodes, threads=threads))
    cpu0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    error = None
    log_path = folder / 'kqprun.log'
    with log_path.open('w') as output:
        process = subprocess.Popen(cmd, cwd=KQP, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    samples = []
    try:
        while True:
            samples.append(sample(process.pid))
            if process.poll() is not None:
                error = f'kqprun exited with {process.returncode}'
                break
            log = ANSI.sub('', log_path.read_text(errors='replace'))
            finished = len(DONE.findall(log)) + len(set(FAIL.findall(log)))
            if 'All async requests finished' in log or finished >= QUERIES:
                break
            if time.monotonic() - start > TIMEOUT:
                error = 'query timeout'
                break
            meminfo = dict(row.split(':', 1) for row in Path('/proc/meminfo').read_text().splitlines())
            if int(meminfo['MemAvailable'].split()[0]) < 4 * 1024 ** 2:
                error = 'free memory guard'
                break
            time.sleep(0.5)
    finally:
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
    # Request ids start at 1 in the order the -p files were given, q01..q22.
    by_query = {int(n): float(t) for n, t in DONE.findall(log)}
    failed = sorted({int(n) for n in FAIL.findall(log)})
    durations = [by_query[n] for n in sorted(by_query)]
    summary = dict(
        label=label, enabled=enabled, nodes=nodes, threads=threads, error=error,
        cpu_seconds=cpu1.ru_utime - cpu0.ru_utime + cpu1.ru_stime - cpu0.ru_stime,
        user_seconds=cpu1.ru_utime - cpu0.ru_utime, system_seconds=cpu1.ru_stime - cpu0.ru_stime,
        wall_seconds=time.monotonic() - start,
        peak_rss_gb=max((s.get('rss') or 0 for s in samples), default=0) / 1024 ** 3,
        finished=by_query, failed=failed,
        mean_seconds=(sum(durations) / len(durations) if durations else None),
        last_seconds=(max(durations) if durations else None),
    )
    save(folder / 'summary.json', summary)
    print(json.dumps({k: v for k, v in summary.items() if k != 'finished'}), flush=True)
    return summary


def main():
    binary = ROOT / 'kqprun'
    current = (KQP / 'kqprun').resolve(strict=True)
    if binary.exists() and binary.stat().st_ino != current.stat().st_ino:
        binary.unlink()
    if not binary.exists():
        os.link(current, binary)
    saved = ROOT / 'runs.json'
    runs = json.loads(saved.read_text()) if saved.exists() else []
    plan = [arg.split(':') for arg in sys.argv[1:]] or [['all-off', 'off'], ['all-on', 'on', '1', '1']]
    for item in plan:
        label, mode = item[0], item[1]
        extra = dict(zip(('shards', 'inflight', 'threads', 'nodes'), map(int, item[2:])))
        if any(r['label'] == label for r in runs):
            continue
        runs.append(run(label, mode == 'on', binary, **extra))
        save(saved, runs)


if __name__ == '__main__':
    main()
