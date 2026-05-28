# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import json
from urllib.parse import urlencode

import requests
import yatest.common


requests.packages.urllib3.disable_warnings()

TOKENS = [
    None,
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

DATABASE = '/Root'
TENANT_DATABASE = '/Root/Tenant'

ENDPOINT_SPECS = [
    {'path': '/actors/'},
    {'path': '/actors/tablet_counters_aggregator'},
    {'path': '/counters'},
    {'path': '/counters/hosts'},
    {'path': '/followercounters'},
    {
        'path': '/healthcheck',
        'queries': [
            {},
            {'format': 'prometheus'},
            {'database': DATABASE},
            {'database': TENANT_DATABASE},
            {'database': DATABASE, 'format': 'prometheus'},
            {'database': TENANT_DATABASE, 'format': 'prometheus'},
        ],
    },
    {'path': '/internal'},
    {'path': '/labeledcounters'},
    {'path': '/login'},
    {'path': '/monitoring/'},
    {'path': '/ping'},
    {'path': '/static/css/bootstrap.min.css'},
    {'path': '/status'},
    {'path': '/ver'},
    {'path': '/viewer/capabilities'},
    {
        'path_factory': lambda cluster: f'/node/{cluster.nodes[1].node_id}/monitoring',
    },
]

_DEFAULT_METHODS = ('GET', 'POST')
_REQUEST_TIMEOUT = 1
_MAX_PARALLEL_REQUESTS = 16

_DEFAULT_QUERIES = [
    {},
    {'database': DATABASE},
    {'database': TENANT_DATABASE},
]


def _methods_for_spec(spec):
    return tuple(spec.get('methods', _DEFAULT_METHODS))


def _queries_for_spec(spec):
    if 'queries' in spec:
        return spec['queries']
    return _DEFAULT_QUERIES


def _base_path(cluster, spec):
    if 'path' in spec:
        return spec['path']
    return spec['path_factory'](cluster)


def _query_string(query):
    if not query:
        return ''
    return f'?{urlencode(query, safe="/")}'


def _full_path(base_path, query):
    return f'{base_path}{_query_string(query)}'


def _requests_for_spec(cluster, spec):
    base_path = _base_path(cluster, spec)
    return [
        (base_path, _query_string(query), _full_path(base_path, query))
        for query in _queries_for_spec(spec)
    ]


def _request_status(method, base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.request(
        method,
        base_url + path,
        headers=headers,
        verify=False,
        timeout=_REQUEST_TIMEOUT,
    )
    return response.status_code


def _collect_endpoints(cluster):
    node = cluster.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    endpoint_specs = []
    for spec in ENDPOINT_SPECS:
        methods = _methods_for_spec(spec)
        for base_path, query_string, full_path in _requests_for_spec(cluster, spec):
            endpoint_specs.append((base_path, query_string, full_path, methods))
    requests_to_run = [
        (base_path, method, query_string, full_path, token)
        for base_path, query_string, full_path, methods in endpoint_specs
        for method in methods
        for token in TOKENS
    ]
    results = {}
    for base_path, query_string, _, methods in endpoint_specs:
        for method in methods:
            results.setdefault(method, {})
            results[method].setdefault(base_path, {})
            results[method][base_path][query_string] = {}
    with ThreadPoolExecutor(max_workers=_MAX_PARALLEL_REQUESTS) as executor:
        futures = [
            executor.submit(_request_status, method, base_url, path, token)
            for _, method, _, path, token in requests_to_run
        ]
        for (base_path, method, query_string, _, token), future in zip(requests_to_run, futures):
            label = token if token is not None else '__none__'
            results[method][base_path][query_string][label] = future.result()
    return results


def _canonize(name, results):
    out_path = yatest.common.output_path(f'{name}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
        f.write('\n')
    return yatest.common.canonical_file(out_path, local=True, universal_lines=True)


def test_mon_endpoints_auth(ydb_cluster_for_mon_endpoints_auth):
    case_name, cluster = ydb_cluster_for_mon_endpoints_auth
    return _canonize(
        f'mon_endpoints_auth-{case_name}',
        _collect_endpoints(cluster),
    )
