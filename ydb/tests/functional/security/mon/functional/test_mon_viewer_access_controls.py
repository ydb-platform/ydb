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


def _build_endpoint_path(endpoint, with_database_cgi, extra_params=None):
    params = dict(extra_params or {})
    if with_database_cgi:
        params = {'database': DATABASE, **params}
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
        # Empty target skips graph request and returns a 1x1 PNG placeholder.
        {'path': '/viewer/render?target=', 'method': EndpointMethod.POST},
        {'path': '/viewer/json/render?target=', 'method': EndpointMethod.POST},
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


# database@builtin is a strict database-only token and must be rejected when path is out of database scope.
def test_viewer_describe_out_of_scope_path(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'path': '/Other'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)


# Only CGI params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
def test_viewer_describe_strict_database_token_extra_params(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'merge': 'true'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)
        _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 400)


# path_id CGI param bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_strict_database_token_forbidden_params(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'path_id': '1'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)
        # path_id is rejected only for some access levels
        _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 200)


# Path outside database scope gives endpoint validation error (400), not role-denied (403).
def test_out_of_scope_path_nodes_gives_400(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/nodes', '/viewer/json/nodes']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'path': '/Other'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)


# schemeshard_id CGI param bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_schemeshard_id_forbidden(mon_base_url_with_extra_sids_control):
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = _build_endpoint_path(ep, with_database_cgi=True, extra_params={'schemeshard_id': '1'})
        _assert_status(mon_base_url_with_extra_sids_control, path, 'database@builtin', 400)
        # An administrator also gets 400, but from handler validation
        _assert_status(mon_base_url_with_extra_sids_control, path, 'root@builtin', 400)
