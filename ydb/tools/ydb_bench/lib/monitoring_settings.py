"""Versioned monitoring configuration shared by benchmark hosts."""

import copy
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, ProxyHandler, build_opener

from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.hosts import NoRedirect


def validate_settings(value):
    required = {'prometheus_url', 'prometheus_token', 'grafana_url'}
    optional = {'grafana_api_url', 'grafana_token', 'grafana_datasource_uid'}
    if not isinstance(value, dict) or not required <= set(value) or set(value) - required - optional:
        raise BenchmarkError('invalid monitoring settings fields')
    result = dict.fromkeys(optional, '')
    result.update(value)
    for key in ('prometheus_token', 'grafana_token'):
        token = result[key]
        if not isinstance(token, str) or len(token) > 8192 or any(ord(c) < 33 or ord(c) > 126 for c in token):
            raise BenchmarkError('invalid {} token'.format('Prometheus' if key == 'prometheus_token' else 'Grafana'))
    uid = result['grafana_datasource_uid']
    if not isinstance(uid, str) or len(uid) > 128 or any(not (c.isascii() and (c.isalnum() or c in '_-')) for c in uid):
        raise BenchmarkError('invalid Grafana datasource UID')
    for key in ('prometheus_url', 'grafana_url', 'grafana_api_url'):
        url = result[key]
        if not isinstance(url, str) or len(url) > 2048:
            raise BenchmarkError('invalid monitoring URL')
        if not url:
            continue
        try:
            parsed = urlsplit(url)
            if (
                parsed.scheme not in ('http', 'https')
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or any(c.isspace() for c in url)
                or parsed.port == 0
            ):
                raise ValueError()
        except ValueError as error:
            raise BenchmarkError('monitoring URLs require HTTP(S), without credentials or query parameters') from error
        result[key] = url.rstrip('/')
    return result


def exchange(host, operation, value=None, timeout=5, max_bytes=65536):
    request = Request(
        host['endpoint'] + '/peer/monitoring/' + operation,
        data=json.dumps(value or {}).encode(),
        headers={'Authorization': 'Bearer ' + host['token'], 'Content-Type': 'application/json'},
    )
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=timeout) as response:
            body = response.read(max_bytes + 1)
            if len(body) > max_bytes:
                raise ValueError('oversized response')
            return json.loads(body)
    except (OSError, ValueError) as error:
        raise BenchmarkError('Monitoring settings exchange failed; refresh before retrying') from error


class MonitoringSettings:
    def __init__(self, output, hosts):
        self.path = Path(output) / '.monitoring-settings.json'
        self.hosts = hosts
        self.lock = threading.RLock()
        self.sync_lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = None
        self.peers = []
        self.error = ''
        self.value = {
            'owner_id': '',
            'revision': 0,
            'settings': validate_settings(dict(prometheus_url='', prometheus_token='', grafana_url='')),
        }
        if self.path.exists():
            self.value = self._validate(json.loads(self.path.read_text()))

    @staticmethod
    def _validate(value):
        if not isinstance(value, dict) or set(value) not in (
            {'owner_id', 'revision', 'settings'},
            {'owner_id', 'revision', 'settings', 'prometheus_revision'},
        ):
            raise BenchmarkError('invalid monitoring settings snapshot')
        if not isinstance(value['owner_id'], str) or type(value['revision']) is not int or value['revision'] < 0:
            raise BenchmarkError('invalid monitoring settings revision')
        if value['revision'] and not value['owner_id']:
            raise BenchmarkError('missing monitoring settings owner')
        generation = value.get('prometheus_revision', 0)
        if type(generation) is not int or not 0 <= generation <= value['revision']:
            raise BenchmarkError('invalid Prometheus revision')
        settings = value['settings']
        if isinstance(settings, dict) and set(settings) == {'enabled', 'import_url', 'grafana_url'}:
            # The legacy import service URL is not a Prometheus endpoint.
            settings = dict(prometheus_url='', prometheus_token='', grafana_url=settings['grafana_url'])
        return dict(value, settings=validate_settings(settings))

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.value)

    def status(self):
        with self.lock:
            value = self.snapshot()
            for key in ('prometheus_token', 'grafana_token'):
                token = value['settings'].pop(key)
                value['settings']['has_' + key] = bool(token)
                value['settings'][key + '_mask'] = ('****' + token[-4:] if len(token) > 4 else '****') if token else ''
            value.update(hosts=copy.deepcopy(self.peers), error=self.error)
            value['owner_id'] = value['owner_id'] or min([self.hosts.id] + [h['id'] for h in self.hosts.list()])
            value['local_id'] = self.hosts.id
            return value

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._watch, daemon=True)
            self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=6)

    def _watch(self):
        while not self.stop.is_set():
            try:
                self.sync()
            except (BenchmarkError, OSError, ValueError):
                with self.lock:
                    self.error = 'Could not synchronize monitoring settings'
            if self.stop.wait(30):
                break

    def sync(self):
        with self.sync_lock:
            hosts = self.hosts.list()

            def read(host):
                try:
                    return host, self._validate(exchange(self.hosts.get(host['id']), 'snapshot'))
                except BenchmarkError:
                    return host, None

            with ThreadPoolExecutor(max_workers=4) as pool:
                snapshots = list(pool.map(read, hosts))
            with self.lock:
                owners = {v['owner_id'] for _, v in snapshots if v and v['revision']}
                if self.value['revision']:
                    owners.add(self.value['owner_id'])
                self.peers = [
                    dict(id=h['id'], name=h['name'], revision=v['revision'] if v else None) for h, v in snapshots
                ]
                if len(owners) > 1:
                    self.error = 'Conflicting settings owners; configuration was not changed'
                    raise BenchmarkError(self.error)
                owner = next(iter(owners), min([self.hosts.id] + [h['id'] for h in hosts]))
                authoritative = next((v for h, v in snapshots if h['id'] == owner and v), None)
                if owner != self.hosts.id and authoritative and authoritative['revision'] > self.value['revision']:
                    if authoritative['owner_id'] != owner:
                        raise BenchmarkError('settings owner mismatch')
                    atomic_write_json(self.path, authoritative)
                    self.value = authoritative
                self.error = ''
                return owner, all(v is not None for _, v in snapshots)

    def save(self, request):
        if not isinstance(request, dict) or set(request) != {'revision', 'settings'}:
            raise BenchmarkError('invalid settings update')
        supplied = request['settings']
        if not isinstance(supplied, dict):
            raise BenchmarkError('invalid settings update')
        candidate = dict(supplied)
        if candidate.get('prometheus_token') is None:
            candidate['prometheus_token'] = ''
        if candidate.get('grafana_token') is None:
            candidate['grafana_token'] = ''
        settings = validate_settings(candidate)
        if type(request['revision']) is not int:
            raise BenchmarkError('revision must be an integer')
        owner, reachable = self.sync()
        if not self.snapshot()['revision'] and not reachable:
            raise BenchmarkError('All hosts must be reachable to initialize cluster settings')
        if owner != self.hosts.id:
            result = exchange(self.hosts.get(owner), 'save', request)
            self.sync()
            return result
        with self.lock:
            if request['revision'] != self.value['revision']:
                raise BenchmarkError('Settings changed; reload before saving')
            if supplied.get('prometheus_token') is None:
                settings['prometheus_token'] = self.value['settings']['prometheus_token']
            for key in ('grafana_token', 'grafana_api_url', 'grafana_datasource_uid'):
                if key not in supplied or (key == 'grafana_token' and supplied[key] is None):
                    settings[key] = self.value['settings'].get(key, '')
            value = dict(owner_id=self.hosts.id, revision=self.value['revision'] + 1, settings=settings)
            changed = any(
                settings[key] != self.value['settings'][key] for key in ('prometheus_url', 'prometheus_token')
            )
            value['prometheus_revision'] = value['revision'] if changed else self.value.get('prometheus_revision', 0)
            atomic_write_json(self.path, value)
            self.value = value
        return self.status()
