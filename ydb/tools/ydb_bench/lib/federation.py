"""Read-through federation. Durable results remain on their owning hosts."""

import json
import base64
import hashlib
import math
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.hosts import request_peer
from ydb.tools.ydb_bench.lib.run_index import order_value


def page_options(query):
    filters = {
        key: query[key][-1]
        for key in ('status', 'source', 'benchmark', 'profile', 'since', 'until', 'query')
        if query.get(key)
    }
    order = query.get('sort', ['newest'])[-1]
    try:
        limit = int(query.get('limit', ['50'])[-1])
    except ValueError as error:
        raise BenchmarkError('invalid page size') from error
    if order not in ('newest', 'oldest', 'longest') or not 1 <= limit <= 100:
        raise BenchmarkError('invalid run page options')
    selected = query.get('selected', [])
    if len(selected) > 20 or any(len(value) > 600 for value in selected):
        raise BenchmarkError('invalid selected runs')
    if 'selected' in query:
        filters['selected'] = selected
    fingerprint = hashlib.sha256(json.dumps([filters, order, query.get('host')], sort_keys=True).encode()).hexdigest()
    after = None
    cursor = query.get('cursor', [''])[-1]
    if cursor:
        try:
            if len(cursor) > 4096:
                raise ValueError()
            value = json.loads(base64.urlsafe_b64decode(cursor))
            after = value['after']
            if (
                value['filter'] != fingerprint
                or len(after) != 3
                or not isinstance(after[0], (int, float))
                or not math.isfinite(after[0])
                or not all(isinstance(v, str) for v in after[1:])
            ):
                raise ValueError()
        except (ValueError, KeyError, TypeError, OverflowError) as error:
            raise BenchmarkError('invalid or outdated run cursor') from error
    return filters, order, limit, after, fingerprint


def run_cursor(record, order, fingerprint):
    return base64.urlsafe_b64encode(
        json.dumps(
            {'filter': fingerprint, 'after': [order_value(record, order), record['host_id'], record['run_id']]}
        ).encode()
    ).decode()


def reference(host_id, item_id):
    return host_id + ':' + item_id


def split_reference(value, default_host):
    prefix, separator, suffix = value.partition(':')
    if separator:
        try:
            if str(uuid.UUID(prefix)) == prefix:
                if not suffix:
                    raise BenchmarkError('empty host-qualified id')
                return prefix, suffix
        except ValueError:
            pass
    return default_host, value


class Federation:
    def __init__(self, service):
        self.service = service
        self.directory = service.hosts

    def hosts(self):
        return [self.directory.identity(self.directory.port)] + self.directory.list()

    def read(self, host_id, path, local):
        if host_id == self.directory.id:
            return local()
        status, _, body = request_peer(self.directory.get(host_id), path)
        if status != 200:
            raise BenchmarkError('host returned HTTP ' + str(status))
        try:
            return json.loads(body)
        except ValueError as error:
            raise BenchmarkError('invalid host response') from error

    def collect(self, path, local, decorate, selected=None):
        hosts = self.hosts()
        if selected:
            hosts = [host for host in hosts if host['id'] == selected]
            if not hosts:
                raise BenchmarkError('host not found')

        def fetch(host):
            try:
                records = self.read(host['id'], path, local)
                if not isinstance(records, list):
                    raise BenchmarkError('invalid host list')
                return [decorate(record, host) for record in records], None
            except (BenchmarkError, KeyError, TypeError) as error:
                return [], {'host_id': host['id'], 'host_name': host['name'], 'error': str(error)}

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(fetch, hosts))
        return {
            'entries': [row for rows, _ in results for row in rows],
            'errors': [error for _, error in results if error],
        }

    def runs(self, filters, selected=None):
        def decorate(record, host):
            return {
                **record,
                'id': reference(host['id'], record['id']),
                'run_id': record['id'],
                'host_id': host['id'],
                'host_name': host['name'],
            }

        return self.collect(
            '/api/runs?' + urlencode(filters), lambda: self.service.run_list(filters), decorate, selected
        )

    def comparisons(self):
        def decorate(record, host):
            def pair(value):
                owner, run = split_reference(value[0], host['id'])
                return [reference(owner, run), *value[1:]]

            return {
                **record,
                'id': reference(host['id'], record['id']),
                'host_id': host['id'],
                'host_name': host['name'],
                'remote': host['id'] != self.directory.id,
                'profiles': [pair(p) for p in record['profiles']],
                'baseline': pair(record['baseline']),
            }

        return self.collect('/api/saved-comparisons', self.service.saved_comparisons, decorate)

    def run_page(self, query):
        _, order, limit, _, fingerprint = page_options(query)
        hosts = self.hosts()
        if query.get('host'):
            hosts = [host for host in hosts if host['id'] == query['host'][-1]]
            if not hosts:
                raise BenchmarkError('host not found')

        def fetch(host):
            try:
                value = self.read(
                    host['id'], '/api/run-page?' + urlencode(query, doseq=True), lambda: self.service.run_page(query)
                )
                if (
                    not isinstance(value, dict)
                    or not isinstance(value.get('entries'), list)
                    or len(value['entries']) > limit + 1
                ):
                    raise BenchmarkError('host does not support paginated runs; update its benchmark server')
                entries = [
                    {
                        **record,
                        'run_id': record['id'],
                        'id': reference(host['id'], record['id']),
                        'host_id': host['id'],
                        'host_name': host['name'],
                    }
                    for record in value['entries']
                ]
                return entries, value.get('benchmarks', []), value.get('index_error')
            except (BenchmarkError, KeyError, TypeError) as error:
                return [], [], str(error)

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(fetch, hosts))
        entries = sorted(
            (record for rows, _, _ in results for record in rows),
            key=lambda record: (order_value(record, order), record['host_id'], record['run_id']),
        )
        errors = [
            {'host_id': host['id'], 'host_name': host['name'], 'error': error}
            for host, (_, _, error) in zip(hosts, results)
            if error
        ]
        page = entries[:limit]
        # Retry partial pages rather than advancing past unavailable hosts.
        cursor = run_cursor(page[-1], order, fingerprint) if len(entries) > limit and not errors else None
        return {
            'entries': page,
            'next_cursor': cursor,
            'errors': errors,
            'benchmarks': sorted({name for _, names, _ in results for name in names}),
        }

    def profiles(self, references):
        if not references or len(references) > 20 or len(set(references)) != len(references):
            raise BenchmarkError('select between 1 and 20 unique runs')
        groups = {}
        for value in references:
            host, run = split_reference(value, self.directory.id)
            groups.setdefault(host, []).append(run)
        names = {host['id']: host['name'] for host in self.hosts()}
        entries, errors = [], []
        for host_id, runs in groups.items():
            try:
                value = self.read(
                    host_id,
                    '/api/local-ydb-comparison?' + urlencode({'run': runs}, doseq=True),
                    lambda: self.service.local_ydb_comparison(runs),
                )
                entries.extend(
                    {
                        **row,
                        'run': reference(host_id, row['run']),
                        'run_id': row['run'],
                        'host_id': host_id,
                        'host_name': names.get(host_id, host_id),
                    }
                    for row in value['entries']
                )
            except (BenchmarkError, KeyError, TypeError) as error:
                errors.append({'host_id': host_id, 'host_name': names.get(host_id, host_id), 'error': str(error)})
        return {'entries': entries, 'errors': errors}
