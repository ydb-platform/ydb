# -*- coding: utf-8 -*-
from helpers import check_endpoints, check_expected_results


PUBLIC_STATUSES = {
    None: 200,
    'user@builtin': 200,
    'database@builtin': 200,
    'viewer@builtin': 200,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}

ENFORCE_MONITORING_STATUSES = {
    None: 401,
    'user@builtin': 403,
    'database@builtin': 403,
    'viewer@builtin': 403,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}

WITHOUT_ENFORCE_STATUSES = {
    None: 200,
    'user@builtin': 200,
    'database@builtin': 200,
    'viewer@builtin': 200,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}

REQUIRE_COUNTERS_AUTH_STATUSES = {
    None: 401,
    'user@builtin': 403,
    'database@builtin': 403,
    'viewer@builtin': 200,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}

REQUIRE_HEALTHCHECK_PROMETHEUS_STATUSES = {
    None: 401,
    'user@builtin': 403,
    'database@builtin': 403,
    'viewer@builtin': 200,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}

REQUIRE_HEALTHCHECK_MONITORING_STATUSES = {
    None: 401,
    'user@builtin': 403,
    'database@builtin': 403,
    'viewer@builtin': 403,
    'monitoring@builtin': 200,
    'root@builtin': 200,
}


PUBLIC_ENDPOINTS = [
    '/actors/tablet_counters_aggregator',
    # Feature flag
    '/counters',
    # Feature flag
    '/counters/hosts',
    '/followercounters',
    '/labeledcounters',
    '/ping',
    '/status',
    '/viewer/capabilities',
    # Feature flag
    '/healthcheck?format=prometheus',
    # Authorisation page inside
    '/monitoring/',
]

ENFORCE_MONITORING_ENDPOINTS = [
    '/healthcheck',
    '/ver',
    '/static/css/bootstrap.min.css',
    '/internal',
    '/actors/',
]

ADDITIONAL_ACCESS_CASES = {
    '/healthcheck?database=%2FRoot': {
        None: 200,
        'user@builtin': 403,
        'database@builtin': 403,
        'viewer@builtin': 403,
        'monitoring@builtin': 200,
        'root@builtin': 200,
    },
}

WITHOUT_ENFORCE_ENDPOINTS = [
    '/actors/tablet_counters_aggregator',
    '/counters',
    '/counters/hosts',
    '/followercounters',
    '/labeledcounters',
    '/healthcheck?format=prometheus',
    '/healthcheck',
    '/healthcheck?database=%2FRoot',
    '/ping',
    '/status',
    '/ver',
    '/viewer/capabilities',
    '/static/css/bootstrap.min.css',
    '/monitoring/',
    '/internal',
    '/actors/',
]

REQUIRE_COUNTERS_AUTH_ENDPOINTS = [
    '/counters',
    '/counters/hosts',
]

REQUIRE_HEALTHCHECK_PROMETHEUS_ENDPOINTS = [
    '/healthcheck?format=prometheus',
    '/healthcheck?database=%2FRoot&format=prometheus',
]

REQUIRE_HEALTHCHECK_MONITORING_ENDPOINTS = [
    '/healthcheck',
    '/healthcheck?database=%2FRoot',
]

SANITY_PUBLIC_ENDPOINTS = [
    '/ping',
]


def test_public_access(ydb_cluster_with_enforce_user_token):
    check_endpoints(
        ydb_cluster_with_enforce_user_token,
        PUBLIC_ENDPOINTS,
        PUBLIC_STATUSES,
    )


def test_monitoring_access_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    check_endpoints(
        ydb_cluster_with_enforce_user_token,
        ENFORCE_MONITORING_ENDPOINTS,
        ENFORCE_MONITORING_STATUSES,
    )


def test_additional_access_cases_with_enforce_user_token(ydb_cluster_with_enforce_user_token):
    check_expected_results(
        ydb_cluster_with_enforce_user_token,
        ADDITIONAL_ACCESS_CASES,
    )


def test_without_enforce_user_token(ydb_cluster_without_enforce_user_token):
    check_endpoints(
        ydb_cluster_without_enforce_user_token,
        WITHOUT_ENFORCE_ENDPOINTS,
        WITHOUT_ENFORCE_STATUSES,
    )


def test_with_require_counters_authentication(ydb_cluster_with_require_counters_auth):
    check_endpoints(
        ydb_cluster_with_require_counters_auth,
        REQUIRE_COUNTERS_AUTH_ENDPOINTS,
        REQUIRE_COUNTERS_AUTH_STATUSES,
    )
    check_endpoints(
        ydb_cluster_with_require_counters_auth,
        SANITY_PUBLIC_ENDPOINTS,
        PUBLIC_STATUSES,
    )


def test_with_require_healthcheck_authentication(ydb_cluster_with_require_healthcheck_auth):
    check_endpoints(
        ydb_cluster_with_require_healthcheck_auth,
        REQUIRE_HEALTHCHECK_PROMETHEUS_ENDPOINTS,
        REQUIRE_HEALTHCHECK_PROMETHEUS_STATUSES,
    )
    check_endpoints(
        ydb_cluster_with_require_healthcheck_auth,
        REQUIRE_HEALTHCHECK_MONITORING_ENDPOINTS,
        REQUIRE_HEALTHCHECK_MONITORING_STATUSES,
    )
    check_endpoints(
        ydb_cluster_with_require_healthcheck_auth,
        SANITY_PUBLIC_ENDPOINTS,
        PUBLIC_STATUSES,
    )
