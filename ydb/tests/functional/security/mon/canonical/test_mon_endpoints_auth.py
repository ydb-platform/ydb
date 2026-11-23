# -*- coding: utf-8 -*-
import json

import pytest
import yatest.common

from ydb.tests.functional.security.lib.mon_endpoints_helpers import (
    DATABASE,
    MON_ENDPOINTS_AUTH_CASES,
    TENANT_DATABASE,
    collect_endpoints,
)

_NO_QUERY_PARAMS = [
    {},
]

# Change counter handlers defaults (max_counter=10, period=1000ms which causes ~10s per stream).
_COUNTER_ENDPOINT_QUERIES = [
    {'max_counter': '1', 'period': '1'},
    {'max_counter': '1', 'period': '1', 'database': DATABASE},
    {'max_counter': '1', 'period': '1', 'database': TENANT_DATABASE},
]

_PDISK_STORAGE_QUERY = {'node_id': '1', 'pdisk_id': '1'}
_VDISK_STORAGE_QUERY = {'node_id': '1', 'pdisk_id': '1', 'vslot_id': '1000'}

_PDISK_INFO_QUERIES = [
    {},
    _PDISK_STORAGE_QUERY,
]

_VDISK_READ_QUERIES = [
    {},
    _VDISK_STORAGE_QUERY,
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
    {'path': '/memory/statistics'},
    # Some /memory/* endpoints are intentionally omitted here because they are unavailable in ASan builds.
    # They are covered separately in functional tests.
    {'path': '/monitoring/'},
    {
        'path_factory': lambda cluster: f'/node/{cluster.nodes[1].node_id}/monitoring',
    },
    {'path': '/nodetabmon'},
    {'path': '/operation'},
    {'path': '/operation/cancel'},
    {'path': '/operation/forget'},
    {'path': '/operation/get'},
    # The legacy /X/json/Y alias must be protected exactly like the /X/Y path above.
    {'path': '/operation/json/get'},
    {'path': '/operation/list'},
    {'path': '/pdisk/info', 'queries': _PDISK_INFO_QUERIES},
    # The legacy /X/json/Y alias must be protected exactly like the /X/Y path above.
    {'path': '/pdisk/json/info', 'queries': _PDISK_INFO_QUERIES},
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
    # There is no handler for this path, so it must be not found for monitoring and admin levels
    # and it must be unauthorized for database and viewer level (by default they don't have access to http handlers)
    {'path': '/unexisted', 'queries': _NO_QUERY_PARAMS, 'methods': ('GET',)},
    {'path': '/vdisk/blobindexstat', 'queries': _VDISK_READ_QUERIES},
    {'path': '/vdisk/evict'},
    {'path': '/vdisk/getblob', 'queries': _VDISK_READ_QUERIES},
    {'path': '/vdisk/vdiskstat', 'queries': _VDISK_READ_QUERIES},
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


def _canonize(name, results):
    out_path = yatest.common.output_path(f'{name}.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
        f.write('\n')
    return yatest.common.canonical_file(out_path, local=True, universal_lines=True)


def _run_mon_endpoints_auth_test(canon_case_id, cluster):
    return _canonize(
        f'mon_endpoints_auth-{canon_case_id}',
        collect_endpoints(cluster, ENDPOINT_SPECS),
    )


@pytest.mark.parametrize('case_name', MON_ENDPOINTS_AUTH_CASES)
def test(case_name, request):
    cluster = request.getfixturevalue(f'ydb_cluster_mon_endpoints_auth_{case_name}')
    return _run_mon_endpoints_auth_test(request.node.callspec.id, cluster)
