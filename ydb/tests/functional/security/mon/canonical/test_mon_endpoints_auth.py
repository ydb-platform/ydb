# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import json
import threading
from urllib.parse import urlencode

import pytest
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

_DEFAULT_QUERIES = [
    {},
    {'database': DATABASE},
    {'database': TENANT_DATABASE},
]

# Demo counter handlers stream ~10s by default; keep requests short like viewer unit tests.
_COUNTER_ENDPOINT_QUERIES = [
    {'max_counter': '1', 'period': '1'},
    {'max_counter': '1', 'period': '1', 'database': DATABASE},
    {'max_counter': '1', 'period': '1', 'database': TENANT_DATABASE},
]

ENDPOINT_SPECS = [
    {'path': '/actors/'},
    {'path': '/actors/blobstorageproxies'},
    {'path': '/actors/configs_dispatcher'},
    {'path': '/actors/console_configs_provider'},
    {'path': '/actors/dnameserver'},
    {'path': '/actors/dsproxynode'},
    {'path': '/actors/feature_flags'},
    {'path': '/actors/icb'},
    {'path': '/actors/interconnect'},
    {'path': '/actors/kqp_node'},
    {'path': '/actors/kqp_proxy'},
    {'path': '/actors/kqp_resource_manager'},
    {'path': '/actors/kqp_spilling_file'},
    {'path': '/actors/lease'},
    {'path': '/actors/logger'},
    {'path': '/actors/memory_tracker'},
    {'path': '/actors/netclassifier'},
    {'path': '/actors/nodewarden'},
    {'path': '/actors/pdisks'},
    {'path': '/actors/pql2'},
    {'path': '/actors/quoter_proxy'},
    {'path': '/actors/rb'},
    {'path': '/actors/row_dispatcher'},
    {'path': '/actors/schemeboard'},
    {'path': '/actors/sqsgc'},
    {'path': '/actors/statservice'},
    {'path': '/actors/tablet_counters_aggregator'},
    {'path': '/actors/tenant_pool'},
    {'path': '/actors/vdisks'},
    {'path': '/actors/yq_control_plane_proxy'},
    {'path': '/actors/yq_health'},
    {'path': '/cms'},
    {'path': '/counters'},
    {'path': '/counters/hosts'},
    {'path': '/followercounters'},
    {'path': '/fq_diag/fetcher'},
    {'path': '/fq_diag/local_worker_manager'},
    {'path': '/fq_diag/quotas'},
    {'path': '/grpc'},
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
    {'path': '/jquery.tablesorter.css'},
    {'path': '/jquery.tablesorter.js'},
    {'path': '/labeledcounters'},
    {'path': '/login'},
    {'path': '/memory/fragmentation'},
    {'path': '/memory/heap'},
    {'path': '/memory/peakheap'},
    {'path': '/memory/statistics'},
    {'path': '/monitoring/'},
    {
        'path_factory': lambda cluster: f'/node/{cluster.nodes[1].node_id}/monitoring',
    },
    {'path': '/nodetabmon'},
    {'path': '/operation'},
    {'path': '/operation/cancel'},
    {'path': '/operation/forget'},
    {'path': '/operation/get'},
    {'path': '/operation/list'},
    {'path': '/pdisk'},
    {'path': '/pdisk/info'},
    {'path': '/pdisk/restart'},
    {'path': '/pdisk/status'},
    {'path': '/ping'},
    {'path': '/query'},
    {'path': '/query/script/execute'},
    {'path': '/query/script/fetch'},
    {'path': '/scheme'},
    {'path': '/scheme/directory', 'methods': ('GET', 'POST', 'DELETE')},
    {'path': '/static/css/bootstrap.min.css'},
    {'path': '/static/fonts/glyphicons-halflings-regular.eot'},
    {'path': '/static/fonts/glyphicons-halflings-regular.svg'},
    {'path': '/static/fonts/glyphicons-halflings-regular.ttf'},
    {'path': '/static/fonts/glyphicons-halflings-regular.woff'},
    {'path': '/static/js/bootstrap.min.js'},
    {'path': '/static/js/jquery.min.js'},
    {'path': '/status'},
    {'path': '/storage'},
    {'path': '/storage/groups'},
    {'path': '/tablet'},
    {'path': '/tablets'},
    {'path': '/trace'},
    {'path': '/vdisk'},
    {'path': '/vdisk/blobindexstat'},
    {'path': '/vdisk/evict'},
    {'path': '/vdisk/getblob'},
    {'path': '/vdisk/vdiskstat'},
    {'path': '/ver'},
    {'path': '/viewer'},
    {'path': '/viewer/acl'},
    {'path': '/viewer/autocomplete'},
    {'path': '/viewer/browse'},
    {'path': '/viewer/bscontrollerinfo'},
    {'path': '/viewer/bsgroupinfo'},
    {'path': '/viewer/capabilities'},
    {'path': '/viewer/check_access'},
    {'path': '/viewer/cluster'},
    {'path': '/viewer/commit_offset'},
    {'path': '/viewer/compute'},
    {'path': '/viewer/config'},
    {'path': '/viewer/content'},
    {'path': '/viewer/counters'},
    {'path': '/viewer/database_stats'},
    {'path': '/viewer/describe'},
    {'path': '/viewer/describe_consumer'},
    {'path': '/viewer/describe_replication'},
    {'path': '/viewer/describe_topic'},
    {'path': '/viewer/describe_transfer'},
    {'path': '/viewer/feature_flags'},
    {'path': '/viewer/graph'},
    {'path': '/viewer/groups'},
    {'path': '/viewer/healthcheck'},
    {'path': '/viewer/hiveinfo'},
    {'path': '/viewer/hivestats'},
    {'path': '/viewer/hotkeys'},
    {'path': '/viewer/labeledcounters'},
    {'path': '/viewer/metainfo'},
    {'path': '/viewer/multipart_counter', 'queries': _COUNTER_ENDPOINT_QUERIES},
    {'path': '/viewer/netinfo'},
    {'path': '/viewer/nodeinfo'},
    {'path': '/viewer/nodelist'},
    {'path': '/viewer/nodes'},
    {'path': '/viewer/pdiskinfo'},
    {'path': '/viewer/peers'},
    {'path': '/viewer/plan2svg'},
    {'path': '/viewer/pqconsumerinfo'},
    {'path': '/viewer/put_record'},
    {'path': '/viewer/query'},
    {'path': '/viewer/render'},
    {'path': '/viewer/simple_counter', 'queries': _COUNTER_ENDPOINT_QUERIES},
    {'path': '/viewer/sse_counter', 'queries': _COUNTER_ENDPOINT_QUERIES},
    {'path': '/viewer/storage'},
    {'path': '/viewer/storage_stats'},
    {'path': '/viewer/storage_usage'},
    {'path': '/viewer/sysinfo'},
    {'path': '/viewer/tabletinfo'},
    {'path': '/viewer/tabletcounters'},
    {'path': '/viewer/tenantinfo'},
    {'path': '/viewer/tenants'},
    {'path': '/viewer/topic_data'},
    {'path': '/viewer/topicinfo'},
    {'path': '/viewer/v2'},
    {'path': '/viewer/v2/json/config'},
    {'path': '/viewer/v2/json/nodeinfo'},
    {'path': '/viewer/v2/json/nodelist'},
    {'path': '/viewer/v2/json/pdiskinfo'},
    {'path': '/viewer/v2/json/storage'},
    {'path': '/viewer/v2/json/sysinfo'},
    {'path': '/viewer/v2/json/tabletinfo'},
    {'path': '/viewer/v2/json/vdiskinfo'},
    {'path': '/viewer/vdiskinfo'},
    {'path': '/viewer/whoami'},
]

_DEFAULT_METHODS = ('GET', 'POST')
_REQUEST_TIMEOUT = 5
_MAX_PARALLEL_REQUESTS = 32

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
    return [
        (base_path, _query_string(query), _full_path(base_path, query))
        for query in _queries_for_spec(spec)
    ]


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


def _run_mon_endpoints_auth_test(canon_case_id, cluster):
    return _canonize(
        f'mon_endpoints_auth-{canon_case_id}',
        _collect_endpoints(cluster),
    )


_MON_ENDPOINTS_AUTH_CASES = (
    pytest.param('enforce_user_token_enabled_no_schema_grants', id='enforce_user_token_enabled-no_schema_grants'),
    pytest.param('enforce_user_token_enabled_with_schema_grants', id='enforce_user_token_enabled-with_schema_grants'),
    pytest.param('enforce_user_token_disabled_no_schema_grants', id='enforce_user_token_disabled-no_schema_grants'),
    pytest.param('enforce_user_token_disabled_with_schema_grants', id='enforce_user_token_disabled-with_schema_grants'),
    pytest.param('require_counters_authentication_no_schema_grants', id='require_counters_authentication-no_schema_grants'),
    pytest.param('require_counters_authentication_with_schema_grants', id='require_counters_authentication-with_schema_grants'),
    pytest.param('require_healthcheck_authentication_no_schema_grants', id='require_healthcheck_authentication-no_schema_grants'),
    pytest.param(
        'require_healthcheck_authentication_with_schema_grants',
        id='require_healthcheck_authentication-with_schema_grants',
    ),
)


@pytest.mark.parametrize('case_name', _MON_ENDPOINTS_AUTH_CASES)
def test(case_name, request):
    cluster = request.getfixturevalue(f'ydb_cluster_mon_endpoints_auth_{case_name}')
    return _run_mon_endpoints_auth_test(request.node.callspec.id, cluster)
