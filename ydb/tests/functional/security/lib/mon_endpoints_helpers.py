# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import threading
from urllib.parse import urlencode

import pytest
import requests

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

_DEFAULT_QUERIES = [
    {},
    {'database': DATABASE},
    {'database': TENANT_DATABASE},
]

_DEFAULT_METHODS = ('GET', 'POST')
_REQUEST_TIMEOUT = 5
_MAX_PARALLEL_REQUESTS = 8

_thread_local = threading.local()


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
    return [(base_path, _query_string(query), _full_path(base_path, query)) for query in _queries_for_spec(spec)]


def _get_http_session():
    session = getattr(_thread_local, 'session', None)
    if session is None:
        session = requests.Session()
        session.verify = False
        _thread_local.session = session
    return session


def _request_status(method, base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = _get_http_session().request(
        method,
        base_url + path,
        headers=headers,
        timeout=_REQUEST_TIMEOUT,
    )
    return response.status_code


def collect_endpoints(cluster, specs):
    node = cluster.nodes[1]
    base_url = f'https://{node.host}:{node.mon_port}'
    endpoint_specs = []
    for spec in specs:
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


MON_ENDPOINTS_AUTH_CASES = (
    pytest.param('enforce_user_token_enabled_no_schema_grants', id='enforce_user_token_enabled-no_schema_grants'),
    pytest.param('enforce_user_token_enabled_with_schema_grants', id='enforce_user_token_enabled-with_schema_grants'),
    pytest.param('enforce_user_token_disabled_no_schema_grants', id='enforce_user_token_disabled-no_schema_grants'),
    pytest.param('enforce_user_token_disabled_with_schema_grants', id='enforce_user_token_disabled-with_schema_grants'),
    pytest.param(
        'require_counters_authentication_no_schema_grants', id='require_counters_authentication-no_schema_grants'
    ),
    pytest.param(
        'require_counters_authentication_with_schema_grants', id='require_counters_authentication-with_schema_grants'
    ),
    pytest.param(
        'require_healthcheck_authentication_no_schema_grants', id='require_healthcheck_authentication-no_schema_grants'
    ),
    pytest.param(
        'require_healthcheck_authentication_with_schema_grants',
        id='require_healthcheck_authentication-with_schema_grants',
    ),
)
