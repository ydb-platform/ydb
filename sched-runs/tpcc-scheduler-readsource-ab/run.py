#!/usr/bin/env python3
"""TPC-C OFF/ON smoke comparison using the existing PoC server binary."""
import csv
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import statistics
import subprocess
import time
import traceback
import urllib.request

ROOT = Path('/home/mfilitov/tpcc-scheduler-readsource-ab')
DATA_ROOT = Path('/home/mfilitov/tpcc-scheduler-ab')
REPO = Path('/home/mfilitov/ydb-pr56258-local-consumer')
KQP = REPO / 'ydb/tests/tools/kqprun'
CLI_SOURCE = Path('/home/mfilitov/tpcc-scheduler-pipeline-ab/ydb')
CLI = ROOT / 'ydb'
BINARY = KQP / 'kqprun'
GRPC = 28770
MON = 28780
NODES = 4
WAREHOUSES = 10
WARMUP = 30
SECONDS = 120
ACTIVE = []


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temp.replace(path)


def status(phase, **details):
    save(ROOT / 'progress.json', dict(phase=phase, updated_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), **details))
    print(json.dumps(dict(phase=phase, **details), ensure_ascii=False), flush=True)


def stop(process, timeout=60):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=20)


def host_cpu():
    fields = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
    return dict(total=sum(fields), idle=fields[3], steal=fields[7], load1=os.getloadavg()[0])


def metrics(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    ticks = os.sysconf('SC_CLK_TCK')
    rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
    return dict(timestamp=time.time(), cpu_seconds=(int(fields[11]) + int(fields[12])) / ticks,
                rss_bytes=rss)


def check_resources():
    if shutil.disk_usage(ROOT).free < 5 * 1024 ** 3:
        raise RuntimeError('free disk guard (5 GiB)')


def config(folder, enabled):
    content = (KQP / 'configuration/app_config.conf').read_text()
    assert content.count('TableServiceConfig {') == 1
    content = content.replace('TableServiceConfig {', f'''TableServiceConfig {{
  DqChannelVersion: 2
  DqChannelConfig {{
    EnableTaskScheduling: {str(enabled).lower()}
    TaskSchedulerPreferLocalConsumer: true
    TaskSchedulerUseWorkerPool: true
    TaskSchedulerObserveOnly: false
    TaskSchedulerShards: 1
    TaskSchedulerMaxInFlightPerShard: 2
    TaskSchedulerMaxRowsPerRun: 0
    TaskSchedulerWorkerSliceCpuUs: 1000
    TaskSchedulerWorkerActivationUs: 2000
    EnableTaskExecutionTrace: false
  }}''', 1)
    content = content.replace('QueryMemoryLimit: 64424509440', 'QueryMemoryLimit: 4294967296')
    path = folder / 'config.conf'
    path.write_text(content)
    return path


def server(folder, storage, enabled):
    folder.mkdir(exist_ok=True)
    sync = folder / 'sync'
    sync.mkdir(exist_ok=True)
    command = [str(BINARY), '-N', f'{NODES}:1', '--threads', '2', '-c', str(config(folder, enabled)),
               '--storage-size', '16', '--storage-path', str(storage), '-G', str(GRPC), '-M', str(MON),
               '--log-default', 'error', '--backtrace', 'light']
    fast = enabled
    env = dict(os.environ, PORT_SYNC_PATH=str(sync), YDB_DQ_TASK_PREFER_CONSUMERS='0',
               YDB_DQ_TASK_PREFER_READY_QUERIES='0', YDB_DQ_TASK_TAIL_WAKE='1',
               YDB_DQ_TASK_FIFO=str(int(fast)), YDB_DQ_TASK_CHEAP_RUN=str(int(fast)),
               YDB_DQ_TASK_WAIT_FIRST_PIPELINE=str(int(fast)),
               YDB_DQ_TASK_FINISH_ONCE=str(int(fast)),
               YDB_DQ_TASK_WAIT_FIRST_INPUT=str(int(fast)),
               YDB_DQ_TASK_REFRESH_INLINE=str(int(fast)))
    save(folder / 'server-command.json', dict(argv=command, cwd=str(KQP),
        binary_inode=BINARY.stat().st_ino, enabled=enabled, worker_count=2,
        environment={k: env[k] for k in ('PORT_SYNC_PATH', 'YDB_DQ_TASK_PREFER_CONSUMERS', 'YDB_DQ_TASK_PREFER_READY_QUERIES', 'YDB_DQ_TASK_TAIL_WAKE', 'YDB_DQ_TASK_FIFO', 'YDB_DQ_TASK_CHEAP_RUN', 'YDB_DQ_TASK_WAIT_FIRST_PIPELINE', 'YDB_DQ_TASK_FINISH_ONCE', 'YDB_DQ_TASK_WAIT_FIRST_INPUT', 'YDB_DQ_TASK_REFRESH_INLINE')}))
    log = (folder / 'server.log').open('w')
    process = subprocess.Popen(command, cwd=KQP, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    ACTIVE.append(process)
    (folder / 'server.pid').write_text(str(process.pid) + '\n')
    until = time.monotonic() + 240
    while time.monotonic() < until:
        if process.poll() is not None:
            raise RuntimeError(f'server startup failed ({process.returncode}): {folder}/server.log')
        try:
            with socket.create_connection(('127.0.0.1', GRPC), timeout=0.5):
                pass
            # Wait for schema service as well as the listening socket.
            ready = subprocess.run([str(CLI), '-e', f'grpc://localhost:{GRPC}', '-d', '/Root',
                                    'scheme', 'ls', '/Root'], capture_output=True, timeout=15)
            if ready.returncode == 0:
                return process
        except (OSError, subprocess.TimeoutExpired):
            pass
        check_resources()
        time.sleep(1)
    raise RuntimeError('server startup timeout')


def cli(folder, name, args, timeout=600):
    command = [str(CLI), '-e', f'grpc://localhost:{GRPC}', '-d', '/Root',
               'workload', 'tpcc', '--path', 'tpcc/w10', *args]
    save(folder / f'{name}-command.json', command)
    with (folder / f'{name}.stdout').open('w') as out, (folder / f'{name}.stderr').open('w') as err:
        process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        ACTIVE.append(process)
        try:
            code = process.wait(timeout=timeout)
        finally:
            stop(process)
        if code:
            raise RuntimeError(f'{name} failed ({code}): {folder}/{name}.stderr')


def counter_snapshot(folder, name):
    merged = {}
    for node in range(NODES):
        url = f'http://127.0.0.1:{MON+node}/counters/json'
        try:
            with urllib.request.urlopen(url, timeout=15) as response:
                data = json.load(response)
            save(folder / f'{name}-node{node}.json', data)
            for sensor in data.get('sensors', []):
                labels = sensor.get('labels', {})
                component = labels.get('component', '')
                sensor_name = labels.get('sensor', labels.get('name', ''))
                activity = labels.get('activity', '')
                if sensor_name in ('ElapsedMicrosecByActivity', 'ReceivedEventsByActivity') and activity in (
                    'KQP_COMPUTE_ACTOR', 'KQP_SOURCE_READ_ACTOR', 'KQP_SCAN_COMPUTE_ACTOR',
                    'KQP_SCAN_FETCH_ACTOR', 'TX_DATASHARD_ACTOR',
                    'NYql::NDq::(anonymous namespace)::TComputeWorker'):
                    key = sensor_name + '.' + activity
                    value = sensor.get('value')
                    if isinstance(value, (int, float)):
                        merged[key] = merged.get(key, 0) + value
                    continue
                if component not in ('TaskSchedulerWorkers', 'TaskSchedulerPoC'):
                    continue
                key = component + '.' + sensor_name
                value = sensor.get('value')
                if isinstance(value, (int, float)):
                    merged[key] = merged.get(key, 0) + value
        except Exception as exc:
            save(folder / f'{name}-node{node}-error.json', dict(url=url, error=repr(exc)))
    save(folder / f'{name}.json', merged)
    return merged


def parse_json(path):
    text = path.read_text()
    decoder = json.JSONDecoder()
    found = []
    for offset, char in enumerate(text):
        if char == '{':
            try:
                value, _ = decoder.raw_decode(text[offset:])
                if isinstance(value, dict) and 'summary' in value and 'transactions' in value:
                    found.append(value)
            except ValueError:
                pass
    if len(found) != 1:
        raise RuntimeError(f'expected one TPC-C JSON result, got {len(found)} in {path}')
    return found[0]


def execute(label, enabled):
    status('running', run=label, scheduler_enabled=enabled)
    folder = ROOT / label
    folder.mkdir()
    # BlobStorage persists the absolute PDisk path. Restore at the path used
    # during initialization, rather than mounting the copy under a new name.
    storage = DATA_ROOT / 'seed/storage'
    check_resources()
    if storage.exists():
        shutil.rmtree(storage)
    baseline_bytes = sum(p.stat().st_blocks * 512 for p in (DATA_ROOT / 'baseline-storage').rglob('*') if p.is_file())
    deadline = time.monotonic() + 600
    while shutil.disk_usage(ROOT).free < baseline_bytes + 5 * 1024**3:
        if time.monotonic() >= deadline:
            raise RuntimeError('insufficient space for a complete restored snapshot plus reserve')
        time.sleep(5)
    subprocess.run(['cp', '-a', '--reflink=auto', '--sparse=always', str(DATA_ROOT / 'baseline-storage'), str(storage)], check=True)
    process = server(folder, storage, enabled)
    summary = None
    try:
        before = counter_snapshot(folder, 'counters-before')
        command = [str(CLI), '-e', f'grpc://localhost:{GRPC}', '-d', '/Root',
                   'workload', 'tpcc', '--path', 'tpcc/w10', 'run', '-w', str(WAREHOUSES),
                   '--warmup', f'{WARMUP}s', '--time', f'{SECONDS}s', '--threads', '2',
                   '--max-sessions', '64', '--no-delays', '--no-tui', '--format', 'Json',
                   '--highres-histogram', '--extended-stats']
        save(folder / 'run-command.json', command)
        samples = []
        with (folder / 'run.stdout').open('w') as out, (folder / 'run.stderr').open('w') as err:
            client = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            ACTIVE.append(client)
            begin = time.monotonic()
            try:
                while client.poll() is None:
                    if process.poll() is not None:
                        raise RuntimeError(f'server crashed ({process.returncode}) in {label}')
                    samples.append(dict(server=metrics(process.pid), client=metrics(client.pid), host=host_cpu()))
                    if samples[-1]['server']['rss_bytes'] > 40 * 1024 ** 3:
                        raise RuntimeError('server RSS guard (40 GiB)')
                    check_resources()
                    if time.monotonic() - begin > 360:
                        raise RuntimeError('TPC-C run timeout')
                    time.sleep(0.5)
            finally:
                stop(client)
        save(folder / 'cpu-samples.json', samples)
        if client.returncode:
            raise RuntimeError(f'load failed ({client.returncode}) in {label}')
        result = parse_json(folder / 'run.stdout')
        save(folder / 'result.json', result)
        after = counter_snapshot(folder, 'counters-after')
        count = sum(tx['ok_count'] for tx in result['transactions'].values())
        failed = sum(tx['failed_count'] for tx in result['transactions'].values())
        duration = result['summary']['time_seconds']
        start = result['summary']['measure_start_ts']
        cpu = {}
        for role in ('server', 'client'):
            measured = [s[role] for s in samples if start <= s[role]['timestamp'] <= start + duration]
            if len(measured) < 2:
                raise RuntimeError('insufficient CPU samples')
            dt = measured[-1]['timestamp'] - measured[0]['timestamp']
            dc = measured[-1]['cpu_seconds'] - measured[0]['cpu_seconds']
            cpu[role + '_cpu_seconds'] = dc
            cpu[role + '_cpu_sample_seconds'] = dt
            cpu[role + '_average_cores'] = dc / dt
        h = [s['host'] for s in samples if start <= s['server']['timestamp'] <= start + duration]
        total = h[-1]['total'] - h[0]['total']
        host = {k + '_pct': 100 * (h[-1][k] - h[0][k]) / total for k in ('idle', 'steal')}
        host['mean_load1'] = statistics.mean(x['load1'] for x in h)
        summary = dict(label=label, scheduler_enabled=enabled, seconds=duration, host=host,
                       transactions=count, failed=failed, tx_per_second=count / duration,
                       new_orders=result['summary']['new_orders'],
                       new_orders_per_minute=result['summary']['new_orders'] * 60 / duration,
                       cli_tpmc_capped=result['summary']['tpmc'],
                       counters={k: after.get(k, 0) - before.get(k, 0) for k in set(before) | set(after)},
                       **cpu,
                       server_peak_rss_bytes=max(s['server']['rss_bytes'] for s in samples),
                       latencies={k: v['percentiles'] for k, v in result['transactions'].items()})
        save(folder / 'summary.json', summary)
        cli(folder, 'check', ['check', '-w', str(WAREHOUSES)], timeout=600)
        summary['consistency_check_ok'] = True
        save(folder / 'summary.json', summary)
        print(json.dumps(summary), flush=True)
    finally:
        stop(process)
    # The immutable seed is retained; disposable working copies are not needed after a successful run.
    shutil.rmtree(storage)
    return summary


def report(runs):
    save(ROOT / 'runs.json', runs)
    columns = ['label', 'scheduler_enabled', 'seconds', 'transactions', 'failed', 'tx_per_second',
               'new_orders_per_minute', 'server_average_cores', 'client_average_cores', 'server_peak_rss_bytes']
    with (ROOT / 'runs.csv').open('w') as out:
        writer = csv.DictWriter(out, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(runs)
    aggregated = {}
    for mode in ('off', 'fast'):
        group = [r for r in runs if r['label'].startswith(mode + '-')]
        if group:
            aggregated[mode] = {
                key: statistics.median(r[key] for r in group) for key in
                ('tx_per_second', 'new_orders_per_minute', 'server_average_cores', 'client_average_cores')}
    save(ROOT / 'comparison.json', aggregated)


def resume_tpch():
    marker = DATA_ROOT / 'resume-tpch-after-optimization'
    if not marker.exists():
        return
    suite = Path('/home/mfilitov/tpch100-suite-ab')
    old_pid = int((suite / 'suite.pid').read_text())
    stat = Path(f'/proc/{old_pid}/stat')
    if stat.exists() and stat.read_text().split()[2] != 'Z':
        return
    with (suite / 'suite.log').open('a') as out:
        process = subprocess.Popen(['python3', str(suite / 'run_suite.py')], stdout=out,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    (suite / 'suite.pid').write_text(str(process.pid) + '\n')
    marker.rename(ROOT / 'tpch-resumed')


def main():
    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    status('waiting-for-build')
    while True:
        setup = json.loads((ROOT / 'build-status.json').read_text())
        if setup['phase'] == 'ready':
            break
        if setup['phase'] == 'failed':
            raise RuntimeError('kqprun build or scheduler tests failed: build-full.log')
        time.sleep(2)
    for port in [GRPC, *range(MON, MON + NODES)]:
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('127.0.0.1', port))
    if not CLI.exists():
        os.link(CLI_SOURCE.resolve(strict=True), CLI)
    if not BINARY.exists():
        raise RuntimeError(f'missing server binary {BINARY}')
    subprocess.run([str(CLI), 'workload', 'tpcc', '--help'], check=True, capture_output=True)
    save(ROOT / 'manifest.json', dict(warehouses=WAREHOUSES, nodes=NODES, user_threads_per_node=2,
         client_threads=2, max_sessions=64, warmup_seconds=WARMUP, measure_seconds=SECONDS,
         repetitions=2, run_order=['off', 'fast', 'fast', 'off'], no_delays=True,
         server=str(BINARY), server_inode=BINARY.stat().st_ino, cli=str(CLI),
         binary_parent_commit='68e47db1d732ecadd2b268b8476551e9c549622b',
         storage_size_gib=16, immutable_seed=True, affinity_changes=False,
         source_data=str(DATA_ROOT), optimization='OFF vs FIFO workers also running KqpReadRangesSource tasks and datashard scans, 2 workers',
         cpu_measurement='Server and client /proc CPU, sampled every 0.5 s during measured interval',
         throughput='Successful transactions / reported measurement duration; new orders / minute computed from counts. CLI tpmc caps at 12.86 * warehouses.',
         scheduler_coverage='KqpReadRangesSource tasks and datashard scans use the worker pool. Actor elapsed is ElapsedMicrosecByActivity for compute, source read, scan and datashard.'))
    seed = DATA_ROOT / 'seed'
    seed.mkdir(exist_ok=True)
    if not (seed / 'complete.json').exists():
        status('importing', warehouses=WAREHOUSES)
        process = server(seed, seed / 'storage', False)
        try:
            cli(seed, 'init', ['init', '-w', str(WAREHOUSES)])
            cli(seed, 'import', ['import', '-w', str(WAREHOUSES), '--threads', '4', '--compact', '--no-tui'], timeout=1800)
            cli(seed, 'check', ['check', '-w', str(WAREHOUSES), '--just-imported'])
        finally:
            stop(process)
        save(seed / 'complete.json', dict(warehouses=WAREHOUSES, initial_check_ok=True))
    baseline = DATA_ROOT / 'baseline-storage'
    if not (DATA_ROOT / 'baseline-complete.json').exists():
        if baseline.exists():
            raise RuntimeError('incomplete baseline copy; inspect before retrying')
        subprocess.run(['cp', '-a', '--reflink=auto', '--sparse=always', str(seed / 'storage'), str(baseline)], check=True)
        save(DATA_ROOT / 'baseline-complete.json', dict(source_absolute_path=str(seed / 'storage')))
    runs = json.loads((ROOT / 'runs.json').read_text()) if (ROOT / 'runs.json').exists() else []
    for label, mode in [('off-1', False), ('fast-1', True), ('fast-2', True), ('off-2', False)]:
        if any(r['label'] == label for r in runs):
            continue
        runs.append(execute(label, mode))
        report(runs)
    status('completed', runs=len(runs), consistency_checks_ok=all(r['consistency_check_ok'] for r in runs))


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt('TPC-C benchmark interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        main()
    except BaseException as exc:
        status('failed', error=repr(exc))
        traceback.print_exc()
    finally:
        for process in reversed(ACTIVE):
            stop(process)
