# -*- coding: utf-8 -*-
import pytest
import yatest.common

from ydb.tests.functional.security.lib.mon_endpoints_helpers import (
    MON_ENDPOINTS_AUTH_CASES,
    TENANT_DATABASE,
    collect_endpoints,
)

ENDPOINT_SPECS = [
    {'path': '/memory/fragmentation'},
    {'path': '/memory/heap'},
    {'path': '/memory/peakheap'},
]


@pytest.mark.parametrize('case_name', MON_ENDPOINTS_AUTH_CASES)
def test(case_name, request):
    cluster = request.getfixturevalue(f'ydb_cluster_mon_endpoints_auth_{case_name}')
    # Sanitized builds use the system allocator, so TCMalloc pages are not registered.
    authorized_status = 404 if yatest.common.context.sanitize else 200
    with_schema_grants = case_name.endswith('_with_schema_grants')

    for method, paths in collect_endpoints(cluster, ENDPOINT_SPECS).items():
        for path, queries in paths.items():
            for query, statuses in queries.items():
                for token, status in statuses.items():
                    if token == '__none__':
                        expected = 401
                    elif token not in ('monitoring@builtin', 'root@builtin'):
                        expected = 403
                    elif query == f'?database={TENANT_DATABASE}' and not with_schema_grants:
                        expected = 403
                    else:
                        expected = authorized_status
                    assert (
                        status == expected
                    ), f'{method} {path}{query}, token={token}: expected {expected}, got {status}'
