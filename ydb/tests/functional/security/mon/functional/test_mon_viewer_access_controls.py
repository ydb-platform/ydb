# -*- coding: utf-8 -*-
from enum import Enum
from urllib.parse import urlencode

import pytest
import requests

from ydb.tests.functional.security.lib.security_test_helpers import (
    DATABASE,
    grant_describe_schema_provided,
    grants_provided,
    with_topic,
)

requests.packages.urllib3.disable_warnings()

TOPIC_NAME = 'topic'
TOPIC_PATH = f'{DATABASE}/{TOPIC_NAME}'


class EndpointMethod(Enum):
    GET = 'get'
    POST = 'post'


def _assert_status(base_url, path, token, status, method=EndpointMethod.GET):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    if method == EndpointMethod.GET:
        response = requests.get(base_url + path, headers=headers, verify=False, timeout=5)
    else:
        response = requests.post(base_url + path, headers=headers, verify=False, timeout=5)
    assert response.status_code == status


def _assert_viewer_query_post(base_url, token, status=200, database=DATABASE):
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json',
    }
    body = {
        'query': 'SELECT 1;',
        'schema': 'multi',
    }
    if database is not None:
        body['database'] = database
    response = requests.post(
        base_url + '/viewer/query',
        headers=headers,
        json=body,
        verify=False,
        timeout=5,
    )
    assert response.status_code == status, response.text


def _build_endpoint_path(endpoint, with_database_cgi, extra_params=None, database=DATABASE):
    params = dict(extra_params or {})
    if with_database_cgi:
        params = {'database': database, **params}
    if not params:
        return endpoint
    separator = '&' if '?' in endpoint else '?'
    return endpoint + separator + urlencode(params)


def _build_topic_path(endpoint, with_database_cgi):
    return _build_endpoint_path(
        endpoint,
        with_database_cgi=with_database_cgi,
        extra_params={
            'path': TOPIC_PATH,
            'partition': 0,
            'offset': 0,
            'limit': 1,
        },
    )


@pytest.fixture
def topic_created(mon_base_url_with_extra_sids_control):
    with with_topic(mon_base_url_with_extra_sids_control, TOPIC_NAME):
        yield


# The capabilities handler is used to discover capabilities, including whether authentication
# is required at all, so it must be available without authentication regardless of the
# enable_extra_sids_control_for_http_viewer feature flag.
def test_capabilities_available_without_auth(
    mon_base_url_with_extra_sids_control,
    mon_base_url_without_extra_sids_control,
):
    for base_url in (mon_base_url_with_extra_sids_control, mon_base_url_without_extra_sids_control):
        for ep in ['/viewer/capabilities', '/viewer/json/capabilities']:
            _assert_status(base_url, ep, None, 200)
            _assert_status(base_url, ep, 'user@builtin', 200)


# External viewer access controls move these endpoints to viewer-level access.
def test_viewer_config_access_controls(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/config', '/viewer/json/config']:
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'database@builtin', 403)
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'viewer@builtin', 200)
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'monitoring@builtin', 200)
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'root@builtin', 200)


def test_viewer_v2_aliases_access_controls(mon_base_url_with_extra_sids_control, describe_schema_grants):
    config_v2_endpoint = '/viewer/v2/json/config'

    for ep in [
        config_v2_endpoint,
        _build_endpoint_path(config_v2_endpoint, with_database_cgi=True),
    ]:
        # no access
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'database@builtin', 403)
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'viewer@builtin', 403)
        # with access
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'monitoring@builtin', 200)
        _assert_status(mon_base_url_with_extra_sids_control, ep, 'root@builtin', 200)

    sysinfo_v2_endpoint = '/viewer/v2/json/sysinfo'
    # no database CGI-param for database_allowed_sids level
    _assert_status(mon_base_url_with_extra_sids_control, sysinfo_v2_endpoint, 'database@builtin', 400)
    # with database CGI-param for database_allowed_sids level
    _assert_status(
        mon_base_url_with_extra_sids_control,
        _build_endpoint_path(sysinfo_v2_endpoint, with_database_cgi=True),
        'database@builtin',
        200,
    )
    # check with and without database CGI-params for different access levels
    _assert_status(mon_base_url_with_extra_sids_control, sysinfo_v2_endpoint, 'viewer@builtin', 200)
    _assert_status(
        mon_base_url_with_extra_sids_control,
        _build_endpoint_path(sysinfo_v2_endpoint, with_database_cgi=True),
        'viewer@builtin',
        200,
    )
    _assert_status(mon_base_url_with_extra_sids_control, sysinfo_v2_endpoint, 'monitoring@builtin', 200)
    _assert_status(
        mon_base_url_with_extra_sids_control,
        _build_endpoint_path(sysinfo_v2_endpoint, with_database_cgi=True),
        'monitoring@builtin',
        200,
    )
    _assert_status(
        mon_base_url_with_extra_sids_control,
        _build_endpoint_path(sysinfo_v2_endpoint, with_database_cgi=True),
        'root@builtin',
        200,
    )
    _assert_status(mon_base_url_with_extra_sids_control, sysinfo_v2_endpoint, 'root@builtin', 200)


def test_database_scoped_endpoints_access_controls(mon_base_url_with_extra_sids_control, describe_schema_grants):
    endpoints = [
        {'path': '/viewer/sysinfo', 'method': EndpointMethod.GET},
        {'path': '/viewer/json/sysinfo', 'method': EndpointMethod.GET},
        {'path': '/viewer/feature_flags', 'method': EndpointMethod.GET},
        {'path': '/viewer/json/feature_flags', 'method': EndpointMethod.GET},
    ]

    for endpoint in endpoints:
        path = endpoint['path']
        method = endpoint['method']

        # no database CGI-param for database_allowed_sids level
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=False),
            'database@builtin',
            400,
            method,
        )
        # with database CGI-param for database_allowed_sids level
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=True),
            'database@builtin',
            200,
            method,
        )
        # check with and without database CGI-params for different access levels
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=False),
            'viewer@builtin',
            200,
            method,
        )
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=True),
            'viewer@builtin',
            200,
            method,
        )
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=False),
            'monitoring@builtin',
            200,
            method,
        )
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=True),
            'monitoring@builtin',
            200,
            method,
        )
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=True),
            'root@builtin',
            200,
            method,
        )
        _assert_status(
            mon_base_url_with_extra_sids_control,
            _build_endpoint_path(path, with_database_cgi=False),
            'root@builtin',
            200,
            method,
        )


def test_viewer_query_database_in_post_body(mon_base_url_with_extra_sids_control, describe_schema_grants):
    _assert_viewer_query_post(mon_base_url_with_extra_sids_control, 'database@builtin', status=400, database=None)
    _assert_viewer_query_post(mon_base_url_with_extra_sids_control, 'database@builtin', status=200, database=DATABASE)

    for token in ('viewer@builtin', 'monitoring@builtin', 'root@builtin'):
        _assert_viewer_query_post(mon_base_url_with_extra_sids_control, token, database=None)
        _assert_viewer_query_post(mon_base_url_with_extra_sids_control, token, database=DATABASE)


def test_topic_data_access_controls(mon_base_url_with_extra_sids_control, topic_created):
    endpoints = ['/viewer/topic_data', '/viewer/json/topic_data']

    for ep in endpoints:
        for token in ('database@builtin', 'viewer@builtin', 'monitoring@builtin'):
            _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=True), token, 400)
            _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=False), token, 400)

    with grant_describe_schema_provided(mon_base_url_with_extra_sids_control):
        with grants_provided(
            mon_base_url_with_extra_sids_control,
            TOPIC_PATH,
            'ydb.granular.describe_schema',
            'ydb.granular.select_row',
        ):
            for ep in endpoints:
                # no database CGI-param for database_allowed_sids level
                token = 'database@builtin'
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=False), token, 400)

                # with database CGI-param for database_allowed_sids level
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=True), token, 200)

                # check with and without database CGI-params for different access levels
                token = 'viewer@builtin'
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=False), token, 200)
                token = 'monitoring@builtin'
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=False), token, 200)
                token = 'root@builtin'
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url_with_extra_sids_control, _build_topic_path(ep, with_database_cgi=False), token, 200)


def test_viewer_tenantinfo_show_all_databases_forbidden_for_strict_database_token(
    mon_base_url_with_extra_sids_control,
    tenant_database,
):
    for ep in ['/viewer/tenantinfo', '/viewer/json/tenantinfo']:
        forbidden_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'show_all_databases': 'true'},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, forbidden_path, 'database@builtin', 403)
        # Scope-param validation must not block users above database level.
        for token in ('viewer@builtin', 'monitoring@builtin', 'root@builtin'):
            _assert_status(mon_base_url_with_extra_sids_control, forbidden_path, token, 200)

        allowed_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, allowed_path, 'database@builtin', 200)
        allowed_path_false = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'show_all_databases': 'false'},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, allowed_path_false, 'database@builtin', 200)


# database@builtin is a strict database-only token and must be rejected when path is out of database scope.
def test_viewer_describe_out_of_scope_path(
    mon_base_url_with_extra_sids_control,
    tenant_database,
):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        forbidden_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'path': '/Other'},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, forbidden_path, 'database@builtin', 400)

        allowed_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'path': tenant_database},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, allowed_path, 'root@builtin', 200)


# Only CGI params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
def test_viewer_describe_strict_database_token_extra_params(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'merge': 'true'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)
        _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 400)


# path_id/schemeshard_id params require monitoring+ level access; strict database and viewer tokens get 4xx.
def test_viewer_describe_path_id_forbidden_for_strict_database_token(
    mon_base_url_with_extra_sids_control,
    tenant_database,
    tenant_describe_ids,
):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        # path_id alone is accepted for monitoring+ access level,
        # as well as both params together: path_id and schemeshard_id
        for extra_params in (
            {'path_id': str(tenant_describe_ids['path_id'])},
            {
                'path_id': str(tenant_describe_ids['path_id']),
                'schemeshard_id': str(tenant_describe_ids['schemeshard_id']),
            },
        ):
            path = _build_endpoint_path(
                ep,
                with_database_cgi=True,
                extra_params=extra_params,
                database=tenant_database,
            )
            _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 403)
            _assert_status(mon_base_url_with_extra_sids_control, path, 'viewer@builtin', 403)
            _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 200)
            _assert_status(mon_base_url_with_extra_sids_control, path, 'monitoring@builtin', 200)

        # schemeshard_id alone without path_id: handler gives 403 for users below monitoring level
        # and rejects monitoring+ with 400 (missing path_id)
        path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'schemeshard_id': str(tenant_describe_ids['schemeshard_id'])},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 403)
        _assert_status(mon_base_url_with_extra_sids_control, path, 'viewer@builtin', 403)
        _assert_status(mon_base_url_with_extra_sids_control, path, 'monitoring@builtin', 400)
        _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 400)


# Path outside database scope gives endpoint validation error (400), not role-denied (403).
def test_out_of_scope_path_nodes_gives_400(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/nodes', '/viewer/json/nodes']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'path': '/Other'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)


def test_storage_groups_scope_params_forbidden_for_strict_database_token(
    mon_base_url_with_extra_sids_control,
    tenant_database,
    tenant_nodelist_ids,
    tenant_storage_ids,
    cluster_storage_ids,
    unknown_node_id,
):
    assert tenant_nodelist_ids, 'tenant database must have at least one node'
    base = mon_base_url_with_extra_sids_control

    allowed_cases = (
        {'group_id': str(min(tenant_storage_ids['group_ids']))},
        # both the nodes of the database itself and the nodes holding its storage are in scope
        {'node_id': str(tenant_nodelist_ids[0])},
        {'node_id': str(min(tenant_storage_ids['node_ids']))},
        {'pdisk_id': str(min(tenant_storage_ids['pdisk_ids']))},
        {
            'node_id': str(min(tenant_storage_ids['node_ids'])),
            'pdisk_id': str(min(tenant_storage_ids['pdisk_ids'])),
        },
    )

    foreign_group_ids = cluster_storage_ids['group_ids'] - tenant_storage_ids['group_ids']
    foreign_pdisk_ids = cluster_storage_ids['pdisk_ids'] - tenant_storage_ids['pdisk_ids']
    assert foreign_group_ids, 'the cluster must have a storage group outside the tenant database'
    foreign_pdisk_id = str(min(foreign_pdisk_ids) if foreign_pdisk_ids else 999999)
    forbidden_cases = (
        {'group_id': str(min(foreign_group_ids))},
        {'node_id': str(unknown_node_id)},
        # a pdisk which exists but holds nothing of the database may be absent in a small cluster,
        # then an id of a pdisk that doesn't exist at all is checked instead
        {'pdisk_id': foreign_pdisk_id},
        # every parameter is validated on its own, so a single out of scope one is enough to deny
        {'node_id': str(tenant_nodelist_ids[0]), 'pdisk_id': foreign_pdisk_id},
        {'group_id': str(min(tenant_storage_ids['group_ids'])), 'node_id': str(unknown_node_id)},
    )

    for extra_params in allowed_cases:
        path = _build_endpoint_path(
            '/storage/groups',
            with_database_cgi=True,
            extra_params=extra_params,
            database=tenant_database,
        )
        _assert_status(base, path, 'database@builtin', 200)

    for extra_params in forbidden_cases:
        path = _build_endpoint_path(
            '/storage/groups',
            with_database_cgi=True,
            extra_params=extra_params,
            database=tenant_database,
        )
        _assert_status(base, path, 'database@builtin', 403)
        # Scope-param validation must not block tokens above strict database level.
        for token in ('viewer@builtin', 'monitoring@builtin', 'root@builtin'):
            _assert_status(base, path, token, 200)

    # Without the database parameter the scope can't be determined at all, so a strict database user
    # is rejected by the endpoint validation before the scope check even runs.
    path = _build_endpoint_path(
        '/storage/groups',
        with_database_cgi=False,
        extra_params={'group_id': str(min(tenant_storage_ids['group_ids']))},
    )
    _assert_status(base, path, 'database@builtin', 400)
    _assert_status(base, path, 'root@builtin', 200)


def test_viewer_sysinfo_tabletinfo_node_id_forbidden_for_strict_database_token(
    mon_base_url_with_extra_sids_control,
    tenant_database,
    tenant_nodelist_ids,
    foreign_node_id,
):
    assert tenant_nodelist_ids, 'tenant database must have at least one node'
    tenant_node_id = tenant_nodelist_ids[0]
    endpoints = [
        '/viewer/sysinfo',
        '/viewer/tabletinfo',
    ]
    for ep in endpoints:
        foreign_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'node_id': str(foreign_node_id)},
            database=tenant_database,
        )
        # Scope-param validation must block strict database level tokens.
        _assert_status(mon_base_url_with_extra_sids_control, foreign_path, 'database@builtin', 403)
        # Scope-param validation must NOT block tokens above strict database level.
        for token in ('viewer@builtin', 'monitoring@builtin', 'root@builtin'):
            _assert_status(mon_base_url_with_extra_sids_control, foreign_path, token, 200)

        allowed_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'node_id': str(tenant_node_id)},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, allowed_path, 'database@builtin', 200)

        # Requests with any foreign nodes in the list must be rejected.
        mixed_list_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'node_id': f'{tenant_node_id},{foreign_node_id}'},
            database=tenant_database,
        )
        _assert_status(mon_base_url_with_extra_sids_control, mixed_list_path, 'database@builtin', 403)


# /viewer/tabletinfo may skip the node_id scope check in the base class,
# so this handler handles the node_id scope check by itself.
def test_viewer_tabletinfo_path_with_node_id_for_strict_database_token(
    mon_base_url_with_extra_sids_control,
    tenant_database,
    tenant_nodelist_ids,
    foreign_node_id,
):
    assert tenant_nodelist_ids, 'tenant database must have at least one node'
    base = mon_base_url_with_extra_sids_control
    for ep in ['/viewer/tabletinfo', '/viewer/json/tabletinfo']:
        forbidden_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'path': tenant_database, 'node_id': str(foreign_node_id)},
            database=tenant_database,
        )
        _assert_status(base, forbidden_path, 'database@builtin', 403)
        for token in ('viewer@builtin', 'monitoring@builtin', 'root@builtin'):
            _assert_status(base, forbidden_path, token, 200)

        allowed_path = _build_endpoint_path(
            ep,
            with_database_cgi=True,
            extra_params={'path': tenant_database, 'node_id': str(tenant_nodelist_ids[0])},
            database=tenant_database,
        )
        _assert_status(base, allowed_path, 'database@builtin', 200)
