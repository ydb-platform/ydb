"""Bundled benchmark dashboards and a bounded, server-side Grafana API client."""

import hashlib
import json
import re
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

import library.python.resource as resource

from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.hosts import NoRedirect
from ydb.tools.ydb_bench.lib.monitoring_settings import exchange

RESOURCE_PREFIX = 'ydb/deploy/helm/ydb-prometheus/dashboards/'
BUNDLES = {
    path.removeprefix(RESOURCE_PREFIX).removesuffix('.json'): json.loads(resource.resfs_read(path))['title']
    for path in sorted(resource.resfs_files(RESOURCE_PREFIX))
    if path.endswith('.json')
}
SCOPE = ('bench_host', 'bench_run', 'bench_benchmark', 'bench_profile', 'bench_attempt')


def bundle_uid(name):
    if name not in BUNDLES:
        raise BenchmarkError('unknown bundled dashboard')
    uid = 'ydb-bench-' + name + '-v1'
    if len(uid) > 40:
        uid = 'ydb-bench-' + name[:18] + '-' + hashlib.sha256(name.encode()).hexdigest()[:8] + '-v1'
    return uid


def scoped_expression(expression):
    matchers = ','.join(key + '=~"${' + key + ':regex}"' for key in SCOPE)
    # Protect strings, ranges and grouping labels while adding selectors to bare metrics.
    tokens = re.compile(
        r'"(?:\\.|[^"\\])*"|\{(?:"(?:\\.|[^"\\])*"|[^}"])*\}|\[[^\]]*\]'
        r'|\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)'
        r'|\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?[a-zA-Z]*'
        r'|[a-zA-Z_:][a-zA-Z0-9_:]*'
    )

    def bare_selector(match):
        token = match.group()
        if not re.fullmatch(r'[a-zA-Z_:][a-zA-Z0-9_:]*', token):
            return token
        tail = expression[match.end() :].lstrip()
        if re.match(r'(?:by|without)\s*\(', tail):
            return token
        if tail.startswith(('(', '{')) or token in {
            'and',
            'or',
            'unless',
            'bool',
            'offset',
            'by',
            'without',
            'on',
            'ignoring',
            'group_left',
            'group_right',
        }:
            return token
        return token + '{}'

    expression = tokens.sub(bare_selector, expression)
    result, quoted, escaped = [], False, False
    for character in expression:
        result.append(character)
        if escaped:
            escaped = False
        elif character == '\\' and quoted:
            escaped = True
        elif character == '"':
            quoted = not quoted
        elif character == '{' and not quoted:
            result.append(matchers + ',')
    return ''.join(result).replace('database="$database"', 'database=~"${database:regex}"')


def dashboard(name, datasource_uid):
    uid = bundle_uid(name)
    value = json.loads(resource.resfs_read(RESOURCE_PREFIX + name + '.json'))
    value.update(
        id=None,
        uid=uid,
        title='YDB benchmark · ' + BUNDLES[name],
        version=0,
        refresh='',
        tags=['ydb-bench'],
        links=[],
        editable=True,
    )

    def adapt(item):
        if isinstance(item, dict):
            for key, child in list(item.items()):
                if key == 'expr' and isinstance(child, str):
                    item[key] = scoped_expression(child)
                else:
                    adapt(child)
        elif isinstance(item, list):
            for child in item:
                adapt(child)

    adapt(value['panels'])
    variables = value['templating']['list']
    for variable in variables:
        if variable['name'] == 'ds':
            variable['current'] = dict(text=datasource_uid, value=datasource_uid)
        elif variable['name'] == 'database':
            selector = '{' + ','.join(key + '=~"${' + key + ':regex}"' for key in SCOPE) + '}'
            query = 'label_values(' + selector + ', database)'
            variable.update(
                definition=query,
                query=dict(query=query, refId='StandardVariableQuery'),
                includeAll=True,
                allValue='.*',
                current=dict(text='All', value='$__all'),
            )
        elif variable.get('type') == 'query':
            query_value = variable.get('query', '')
            query = query_value.get('query', '') if isinstance(query_value, dict) else query_value
            label_query = re.fullmatch(r'label_values\(([a-zA-Z_][a-zA-Z0-9_]*)\)', query)
            if label_query:
                query = 'label_values({' + ','.join(key + '=~"${' + key + ':regex}"' for key in SCOPE)
                query += '},' + label_query[1] + ')'
            elif query.startswith('label_values({'):
                query = query.replace('{', '{' + ','.join(key + '=~"${' + key + ':regex}"' for key in SCOPE) + ',', 1)
            else:
                continue
            variable['definition'] = query
            if isinstance(query_value, dict):
                variable['query'] = dict(query_value, query=query)
            else:
                variable['query'] = query
    variables[:0] = [
        dict(
            name=key,
            label=key.removeprefix('bench_').title(),
            type='custom',
            query='',
            includeAll=True,
            allValue='.*',
            multi=False,
            current=dict(text='Not selected', value='__unset__'),
            options=[],
        )
        for key in SCOPE
    ]
    return value


class Grafana:
    def __init__(self, settings, hosts):
        self.settings, self.hosts = settings, hosts

    def config(self):
        snapshot = self.settings.snapshot()
        return dict(configured=bool(snapshot['settings']['grafana_url']), local_id=self.hosts.id)

    @staticmethod
    def _request(settings, path, body=None, missing=False):
        base = settings.get('grafana_api_url') or settings['grafana_url']
        if not base:
            raise BenchmarkError('Grafana is not configured')
        headers = {'Accept': 'application/json'}
        if settings.get('grafana_token'):
            headers['Authorization'] = 'Bearer ' + settings['grafana_token']
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = Request(base + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
        try:
            with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=10) as response:
                if not 200 <= response.status < 300:
                    raise BenchmarkError('Grafana rejected the request')
                payload = response.read(4 * 1024 * 1024 + 1)
                if len(payload) > 4 * 1024 * 1024:
                    raise BenchmarkError('Grafana response is too large')
                return json.loads(payload)
        except HTTPError as error:
            code = error.code
            error.close()
            if missing and code == 404:
                return None
            raise BenchmarkError('Grafana HTTP {}. Check API URL, token and permissions.'.format(code)) from None
        except (OSError, ValueError):
            raise BenchmarkError('Cannot read Grafana API. Check API URL and availability.') from None

    def execute(self, operation, options=None, forwarded=False):
        options = {} if options is None else options
        if operation not in ('catalog', 'install') or not isinstance(options, dict):
            raise BenchmarkError('invalid Grafana operation')
        if (operation == 'catalog' and options) or (
            operation == 'install' and set(options) != {'bundle', 'datasource_uid', 'revision'}
        ):
            raise BenchmarkError('invalid Grafana options')
        snapshot = self.settings.snapshot()
        owner = snapshot['owner_id'] or self.hosts.id
        if owner != self.hosts.id:
            if forwarded:
                raise BenchmarkError('Grafana settings owner changed; refresh settings')
            return exchange(
                self.hosts.get(owner),
                'grafana',
                dict(operation=operation, options=options),
                timeout=30,
                max_bytes=1024 * 1024,
            )
        settings = snapshot['settings']
        if not settings['grafana_url']:
            raise BenchmarkError('Grafana is not configured')
        if operation == 'catalog':
            installed = self._request(settings, '/api/search?type=dash-db&limit=1000')
            sources = self._request(settings, '/api/datasources')
            bundles = {bundle_uid(name): name for name in BUNDLES}
            items = {}
            for item in installed:
                uid = item.get('uid', '')
                if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', uid):
                    continue
                items[uid] = dict(uid=uid, title=item['title'], installed=True, bundle=bundles.get(uid))
            for uid, name in bundles.items():
                items.setdefault(
                    uid, dict(uid=uid, title='YDB benchmark · ' + BUNDLES[name], installed=False, bundle=name)
                )
            return dict(
                url=settings['grafana_url'],
                revision=snapshot['revision'],
                dashboards=list(items.values()),
                truncated=len(installed) == 1000,
                datasource_uid=settings.get('grafana_datasource_uid', ''),
                datasources=[dict(uid=s['uid'], name=s['name']) for s in sources if s.get('type') == 'prometheus'],
            )
        if type(options['revision']) is not int or options['revision'] != snapshot['revision']:
            raise BenchmarkError('Monitoring settings changed; reopen the dashboard chooser')
        name, source = options['bundle'], options['datasource_uid']
        if (
            not isinstance(name, str)
            or not isinstance(source, str)
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', source)
        ):
            raise BenchmarkError('invalid dashboard or datasource')
        uid = bundle_uid(name)
        if self._request(settings, '/api/dashboards/uid/' + uid, missing=True) is not None:
            raise BenchmarkError('Dashboard already exists; reopen the chooser. It was not overwritten.')
        datasource = self._request(settings, '/api/datasources/uid/' + quote(source, safe=''))
        if datasource.get('type') != 'prometheus':
            raise BenchmarkError('A Prometheus datasource is required')
        self._request(settings, '/api/dashboards/db', dict(dashboard=dashboard(name, source), overwrite=False))
        return dict(uid=uid, url=settings['grafana_url'])
