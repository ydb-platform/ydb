"""Background, bounded export of archived YDB samples via Prometheus remote write."""

import hashlib
import json
import math
import re
import sqlite3
import struct
import tempfile
import threading
import time
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.hosts import NoRedirect
from ydb.tools.ydb_bench.lib.ydb_telemetry import read_counters_archive


def _varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _field(number, value):
    return _varint(number * 8 + 2) + _varint(len(value)) + value


def encode_write(samples):
    """Remote Write 1.0 protobuf in a raw Snappy block (literal-only encoding)."""
    payload = bytearray()
    grouped = {}
    for labels, timestamp, value in samples:
        grouped.setdefault(tuple(sorted(labels.items())), []).append((timestamp, value))
    for labels, points in grouped.items():
        series = b''.join(_field(1, _field(1, key.encode()) + _field(2, val.encode())) for key, val in labels)
        for timestamp, value in points:
            sample = b'\x09' + struct.pack('<d', value) + b'\x10' + _varint(timestamp)
            series += _field(2, sample)
        payload.extend(_field(1, series))
    # Raw Snappy supports uncompressed literals; no framed stream or gzip header.
    size = len(payload)
    if not size:
        return b'\x00'
    length = size - 1
    width = max(1, (length.bit_length() + 7) // 8)
    literal = bytes([length << 2]) if length < 60 else bytes([(59 + width) << 2]) + length.to_bytes(width, 'little')
    return _varint(size) + literal + payload


def snapshot_samples(record, identity, node_labels=None):
    if 'error' in record:
        return
    context = record.get('context', {})
    timestamp = int(float(record['timestamp_unix']) * 1000)
    if timestamp < 0:
        raise BenchmarkError('invalid counters timestamp')
    common = dict(
        identity,
        instance='{}:{}'.format(record['host'], record['port']),
        bench_role=str(record['role']),
        bench_node=str(record['index']),
        bench_attempt='verification' if context.get('verification') else str(context.get('attempt', '')),
        bench_repetition=str(context.get('repetition', '')),
    )
    for sensor in record['counters']['sensors']:
        labels = dict(sensor.get('labels', {}))
        name = labels.pop('sensor', None)
        if not name:
            continue
        subsystem = labels.pop('counters', '')
        name = re.sub(r'[^a-zA-Z0-9_:]', '_', str(subsystem) + '_' + str(name) if subsystem else str(name))
        if name[0].isdigit():
            name = '_' + name
        if any(not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*', key) for key in labels):
            raise BenchmarkError('counter contains a label name unsupported by Prometheus')
        if set(labels) & (set(common) | {'__name__'}):
            raise BenchmarkError('counter labels conflict with export identity')
        labels = dict(labels, **common, __name__=name)
        for key, value in (node_labels or {}).items():
            labels.setdefault(key, value)
        labels = {key: str(value) for key, value in labels.items() if str(value)}
        if 'value' in sensor:
            value = float(sensor['value'])
            if not math.isfinite(value):
                raise BenchmarkError('counter contains a non-finite value')
            yield labels, timestamp, value
        elif 'hist' in sensor:
            hist = sensor['hist']
            bounds, buckets = hist.get('bounds', []), hist.get('buckets', [])
            if len(bounds) != len(buckets) or 'le' in labels:
                raise BenchmarkError('invalid histogram')
            total = 0
            for bound, count in zip(bounds, buckets):
                total += count
                yield dict(labels, __name__=name + '_bucket', le=str(bound)), timestamp, float(total)
            total += hist.get('inf', 0)
            yield dict(labels, __name__=name + '_bucket', le='+Inf'), timestamp, float(total)
            yield dict(labels, __name__=name + '_count'), timestamp, float(total)
        else:
            raise BenchmarkError('unsupported counter representation')


class MetricsExporter:
    def __init__(self, output, settings, host_id):
        self.root = Path(output) / '.metrics-exports'
        self.settings, self.host_id = settings, host_id
        self.lock = threading.Lock()
        self.active = None
        self.thread = None

    def _settings(self):
        snapshot = self.settings.snapshot()
        return dict(snapshot['settings'], generation=snapshot.get('prometheus_revision', 0))

    def status(self, run_id, selection, settings=None):
        settings = self._settings() if settings is None else settings
        key = hashlib.sha256(
            json.dumps([run_id, selection, settings['prometheus_url'], settings['generation']], sort_keys=True).encode()
        ).hexdigest()
        path = self.root / (key + '.json')
        with self.lock:
            value = json.loads(path.read_text()) if path.exists() else {'state': 'ready', 'samples': 0}
            if value['state'] in ('preparing', 'exporting') and self.active != key:
                value.update(state='failed', error='Export interrupted. Retry checks stored samples before sending.')
        return dict(value, configured=bool(settings['prometheus_url']), key=key)

    def start(self, run_id, root, selection):
        root = Path(root).resolve()
        settings = self._settings()
        status = self.status(run_id, selection, settings)
        if not status['configured']:
            raise BenchmarkError('Prometheus URL is not configured')
        with self.lock:
            if self.active:
                if self.active == status['key']:
                    return status
                raise BenchmarkError('Another metrics export is running on this host')
            if status['state'] == 'completed':
                return status
            self.root.mkdir(exist_ok=True)
            key = status['key']
            self.active = key
            value = dict(state='preparing', samples=0)
            if 'fingerprint' in status:
                value['fingerprint'] = status['fingerprint']
            atomic_write_json(self.root / (key + '.json'), value)
            self.thread = threading.Thread(
                target=self._run, args=(key, run_id, root, selection, settings, value), daemon=True
            )
            self.thread.start()
        return dict(value, configured=True)

    def _check_settings(self, settings):
        current = self._settings()
        if any(current[key] != settings[key] for key in ('generation', 'prometheus_url', 'prometheus_token')):
            raise BenchmarkError('Prometheus settings changed; start a new export')

    def _skip_existing(self, db, run_id, selection, settings):
        """Read raw samples, not resampled query_range points or series existence."""
        match = dict(bench_host=self.host_id, bench_run=run_id)
        if selection:
            match.update(
                bench_benchmark=selection['benchmark'],
                bench_profile=selection['profile'],
                bench_attempt=selection['attempt'],
            )
        selector = '{' + ','.join(key + '=' + json.dumps(val) for key, val in match.items()) + '}'
        first, last = db.execute('SELECT min(timestamp),max(timestamp) FROM samples').fetchone()
        skipped = 0
        # Range selectors exclude their left boundary. Start one millisecond earlier.
        start = first - 1
        while start < last:
            self._check_settings(settings)
            end = min(start + 30000, last)
            query = selector + '[{}ms]'.format(end - start)
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            if settings['prometheus_token']:
                headers['Authorization'] = 'Bearer ' + settings['prometheus_token']
            request = Request(
                settings['prometheus_url'] + '/api/v1/query',
                data=urlencode(dict(query=query, time=end / 1000)).encode(),
                headers=headers,
            )
            try:
                with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=30) as response:
                    if response.status != 200:
                        raise BenchmarkError('Prometheus sample check failed')
                    body = response.read(64 * 1024 * 1024 + 1)
                if len(body) > 64 * 1024 * 1024:
                    raise BenchmarkError('Prometheus sample check exceeded the response size limit')
                result = json.loads(body)
                if (
                    result.get('status') != 'success'
                    or result.get('warnings')
                    or result.get('data', {}).get('resultType') != 'matrix'
                ):
                    raise BenchmarkError('Prometheus did not return a complete sample check')
                for series in result['data']['result']:
                    labels = json.dumps(series['metric'], sort_keys=True, separators=(',', ':'))
                    row = db.execute('SELECT id FROM series WHERE labels=?', (labels,)).fetchone()
                    if row is None:
                        continue
                    for timestamp, sample in series['values']:
                        timestamp = round(float(timestamp) * 1000)
                        local = db.execute(
                            'SELECT value FROM samples WHERE series=? AND timestamp=?', (row[0], timestamp)
                        ).fetchone()
                        if local is None:
                            continue
                        if local[0] != float(sample):
                            raise BenchmarkError('Prometheus contains a different value for an archived sample')
                        db.execute('DELETE FROM samples WHERE series=? AND timestamp=?', (row[0], timestamp))
                        skipped += 1
            except HTTPError as error:
                code = error.code
                error.close()
                raise BenchmarkError('Prometheus sample check failed: HTTP {}'.format(code)) from None
            db.commit()
            start = end
        return skipped

    def _run(self, key, run_id, root, selection, settings, value):
        path = self.root / (key + '.json')
        try:
            with tempfile.TemporaryDirectory(prefix='metrics-export-', dir=self.root) as temporary:
                with closing(sqlite3.connect(str(Path(temporary) / 'samples.sqlite'))) as db:
                    db.execute('PRAGMA max_page_count=524288')
                    db.execute('PRAGMA cache_size=-8192')
                    db.execute('CREATE TABLE series (id INTEGER PRIMARY KEY, labels TEXT UNIQUE)')
                    db.execute(
                        'CREATE TABLE samples (series INTEGER, timestamp INTEGER, value REAL, PRIMARY KEY(series,timestamp)) WITHOUT ROWID'
                    )
                    manifest = json.loads((root / 'run.json').read_text())
                    for profile in manifest.get('runs', []):
                        benchmark, name = profile.get('benchmark'), profile.get('profile')
                        if benchmark not in ('local-ydb', 'distributed-ydb'):
                            continue
                        if selection and (benchmark != selection['benchmark'] or name != selection['profile']):
                            continue
                        relative = profile.get('manifest') or str(Path(profile.get('directory', '')) / 'run.json')
                        directory = (root / relative).resolve().parent
                        if root not in directory.parents:
                            raise BenchmarkError('profile directory escapes run')
                        profile_path = directory / 'run.json'
                        if root not in profile_path.resolve().parents:
                            raise BenchmarkError('profile manifest escapes run')
                        parameters = (
                            json.loads(profile_path.read_text()).get('parameters', {}) if profile_path.exists() else {}
                        )
                        nodes = parameters.get('distributed', {}).get('template', {}).get('nodes', [])
                        for archive in sorted(directory.rglob('ydb-counters/*.jsonl.gz')):
                            if root not in archive.resolve().parents or archive.is_symlink():
                                raise BenchmarkError('counter archive escapes run')
                            for record in read_counters_archive(archive):
                                context = record.get('context', {})
                                attempt = 'verification' if context.get('verification') else str(context.get('attempt'))
                                if selection and attempt != selection['attempt']:
                                    continue
                                identity = dict(
                                    job='ydb',
                                    bench_host=self.host_id,
                                    bench_run=run_id,
                                    bench_benchmark=benchmark,
                                    bench_profile=name,
                                )
                                node_labels = dict(
                                    host=record['host'],
                                    container='ydb-' + record['role'],
                                    pod='{}-{}'.format(record['role'], record['index']),
                                )
                                if record['role'] == 'dynamic':
                                    if benchmark == 'local-ydb':
                                        node_labels['database'] = '/Root/bench'
                                    elif type(record['index']) is int and 0 < record['index'] <= len(nodes):
                                        node = nodes[record['index'] - 1]
                                        node_labels.update(pod=node['name'], database=node.get('tenant', ''))
                                for labels, timestamp, sample in snapshot_samples(record, identity, node_labels):
                                    encoded = json.dumps(labels, sort_keys=True, separators=(',', ':'))
                                    db.execute('INSERT OR IGNORE INTO series(labels) VALUES (?)', (encoded,))
                                    series = db.execute('SELECT id FROM series WHERE labels=?', (encoded,)).fetchone()[
                                        0
                                    ]
                                    previous = db.execute(
                                        'SELECT value FROM samples WHERE series=? AND timestamp=?', (series, timestamp)
                                    ).fetchone()
                                    if previous and previous[0] != sample:
                                        raise BenchmarkError('conflicting samples for the same series and timestamp')
                                    db.execute(
                                        'INSERT OR IGNORE INTO samples VALUES (?,?,?)', (series, timestamp, sample)
                                    )
                    db.commit()
                    total = db.execute('SELECT count(*) FROM samples').fetchone()[0]
                    if not total:
                        raise BenchmarkError('No archived counters for this selection')
                    digest = hashlib.sha256()
                    ordered = 'SELECT series.labels,samples.timestamp,samples.value FROM samples JOIN series ON series.id=samples.series ORDER BY samples.series,samples.timestamp'
                    for row in db.execute(ordered):
                        digest.update(json.dumps(row, separators=(',', ':')).encode())
                        digest.update(b'\n')
                    fingerprint = digest.hexdigest()
                    if value.get('fingerprint', fingerprint) != fingerprint:
                        raise BenchmarkError('Archived samples changed since the previous export; cannot resume safely')
                    value['fingerprint'] = fingerprint
                    value.update(total=total, phase='checking')
                    atomic_write_json(path, value)
                    skipped = self._skip_existing(db, run_id, selection, settings)
                    value.update(samples=skipped, existing_samples=skipped, phase='writing')
                    value.update(state='exporting', total=total)
                    atomic_write_json(path, value)
                    rows = db.execute(ordered)
                    while True:
                        self._check_settings(settings)
                        batch = rows.fetchmany(10000)
                        if not batch:
                            break
                        body = encode_write(
                            [(json.loads(labels), timestamp, sample) for labels, timestamp, sample in batch]
                        )
                        headers = {
                            'Content-Type': 'application/x-protobuf',
                            'Content-Encoding': 'snappy',
                            'X-Prometheus-Remote-Write-Version': '0.1.0',
                            'User-Agent': 'ydb-bench/1',
                        }
                        if settings['prometheus_token']:
                            headers['Authorization'] = 'Bearer ' + settings['prometheus_token']
                        request = Request(
                            settings['prometheus_url'].rstrip('/') + '/api/v1/write', data=body, headers=headers
                        )
                        for retry in range(4):
                            try:
                                with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=30) as response:
                                    if not 200 <= response.status < 300:
                                        raise BenchmarkError('Prometheus rejected the export')
                                break
                            except HTTPError as error:
                                code = error.code
                                error.close()
                                if retry == 3 or code < 500 and code != 429:
                                    raise BenchmarkError(
                                        'Prometheus HTTP {}. Check remote-write receiver, token and historical sample window.'.format(
                                            code
                                        )
                                    ) from None
                            except OSError:
                                if retry == 3:
                                    raise
                            time.sleep(2**retry)
                        value['samples'] += len(batch)
                        atomic_write_json(path, value)
            value.update(state='completed')
        except Exception as error:
            # Do not expose response bodies, request headers or credentials in status.
            value.update(
                state='failed',
                error=str(error) if isinstance(error, BenchmarkError) else 'Export failed: ' + type(error).__name__,
            )
        finally:
            try:
                atomic_write_json(path, value)
            finally:
                with self.lock:
                    self.active = None
