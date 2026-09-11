"""Read-through federation. Durable results remain on their owning hosts."""

import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.hosts import request_peer


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
                return [reference(owner, run), value[1]]

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
