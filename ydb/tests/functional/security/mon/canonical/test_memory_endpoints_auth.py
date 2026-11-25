# -*- coding: utf-8 -*-
import pytest
import yatest.common

from ydb.tests.functional.security.lib.mon_endpoints_helpers import (
    MON_ENDPOINTS_AUTH_CASES,
    canonize,
    collect_endpoints,
)

ENDPOINT_SPECS = [
    {'path': '/memory/fragmentation'},
    {'path': '/memory/heap'},
    {'path': '/memory/peakheap'},
]


@pytest.mark.skipif(
    bool(yatest.common.context.sanitize),
    reason='TCMalloc memory endpoints are unavailable in sanitized builds',
)
@pytest.mark.parametrize('case_name', MON_ENDPOINTS_AUTH_CASES)
def test(case_name, request):
    cluster = request.getfixturevalue(f'ydb_cluster_mon_endpoints_auth_{case_name}')
    return canonize(
        f'memory_endpoints_auth-{request.node.callspec.id}',
        collect_endpoints(cluster, ENDPOINT_SPECS),
    )
