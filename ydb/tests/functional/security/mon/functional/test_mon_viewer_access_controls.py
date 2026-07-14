# -*- coding: utf-8 -*-
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


def _get_status(base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.get(base_url + path, headers=headers, verify=False)
    return response.status_code


def _post_status(base_url, path, token):
    headers = {}
    if token is not None:
        headers['Authorization'] = token
    response = requests.post(base_url + path, headers=headers, verify=False)
    return response.status_code


def _assert_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) == status
    assert _post_status(base_url, path, token) == status


def _assert_not_status(base_url, path, token, status):
    assert _get_status(base_url, path, token) != status
    assert _post_status(base_url, path, token) != status


def _get_topic_cgi_params(endpoint, *, with_database_cgi=True):
    params = {
        'path': TOPIC_PATH,
        'partition': 0,
        'offset': 0,
        'limit': 1,
    }
    if with_database_cgi:
        params = {'database': DATABASE, **params}
    return endpoint + '?' + urlencode(params)


@pytest.fixture
def topic_created(mon_base_url):
    with with_topic(mon_base_url, TOPIC_NAME):
        yield


# External viewer access controls move these endpoints to viewer-level access.
def test_viewer_config_access_controls(mon_base_url):
    for ep in ['/viewer/config', '/viewer/json/config']:
        _assert_status(mon_base_url, ep, 'database@builtin', 403)
        _assert_not_status(mon_base_url, ep, 'viewer@builtin', 403)
        _assert_not_status(mon_base_url, ep, 'monitoring@builtin', 403)
        _assert_not_status(mon_base_url, ep, 'root@builtin', 403)


def test_viewer_v2_aliases_access_controls(mon_base_url, describe_schema_grants):
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'

    for ep in ['/viewer/v2/json/config', '/viewer/v2/json/config' + db_qs]:
        # no access
        _assert_status(mon_base_url, ep, 'database@builtin', 403)
        _assert_status(mon_base_url, ep, 'viewer@builtin', 403)
        # with access
        _assert_status(mon_base_url, ep, 'monitoring@builtin', 200)
        _assert_status(mon_base_url, ep, 'root@builtin', 200)

    SYSINFO_V2_ENDPOINT = '/viewer/v2/json/sysinfo'
    # no database CGI-param for database_allowed_sids level
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT, 'database@builtin', 400)
    # with database CGI-param for database_allowed_sids level
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT + db_qs, 'database@builtin', 200)
    # check with and without database CGI-params for different access levels
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT, 'viewer@builtin', 200)
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT + db_qs, 'viewer@builtin', 200)
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT, 'monitoring@builtin', 200)
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT + db_qs, 'monitoring@builtin', 200)
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT + db_qs, 'root@builtin', 200)
    _assert_status(mon_base_url, SYSINFO_V2_ENDPOINT, 'root@builtin', 200)


def test_database_scoped_endpoints_access_controls(mon_base_url, describe_schema_grants):
    db_qs = f'?database={DATABASE.replace("/", "%2F")}'

    for ep in ['/viewer/sysinfo', '/viewer/json/sysinfo', '/viewer/json/feature_flags', '/viewer/feature_flags']:
        # no database CGI-param for database_allowed_sids level
        _assert_status(mon_base_url, ep, 'database@builtin', 400)
        # with database CGI-param for database_allowed_sids level
        _assert_status(mon_base_url, ep + db_qs, 'database@builtin', 200)
        # check with and without database CGI-params for different access levels
        _assert_status(mon_base_url, ep, 'viewer@builtin', 200)
        _assert_status(mon_base_url, ep + db_qs, 'viewer@builtin', 200)
        _assert_status(mon_base_url, ep, 'monitoring@builtin', 200)
        _assert_status(mon_base_url, ep + db_qs, 'monitoring@builtin', 200)
        _assert_status(mon_base_url, ep + db_qs, 'root@builtin', 200)
        _assert_status(mon_base_url, ep, 'root@builtin', 200)


def test_topic_data_access_controls(mon_base_url, topic_created):
    endpoints = ['/viewer/topic_data', '/viewer/json/topic_data']

    for ep in endpoints:
        for token in ('database@builtin', 'viewer@builtin', 'monitoring@builtin'):
            _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=True), token, 400)
            _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=False), token, 400)

    with grant_describe_schema_provided(mon_base_url):
        with grants_provided(
            mon_base_url,
            TOPIC_PATH,
            'ydb.granular.describe_schema',
            'ydb.granular.select_row',
        ):
            for ep in endpoints:
                # no database CGI-param for database_allowed_sids level
                token = 'database@builtin'
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=False), token, 400)

                # with database CGI-param for database_allowed_sids level
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=True), token, 200)

                # check with and without database CGI-params for different access levels
                token = 'viewer@builtin'
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=False), token, 200)
                token = 'monitoring@builtin'
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=False), token, 200)
                token = 'root@builtin'
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=True), token, 200)
                _assert_status(mon_base_url, _get_topic_cgi_params(ep, with_database_cgi=False), token, 200)


# database@builtin is a strict database-only token and must be rejected when path is out of database scope.
def test_viewer_describe_out_of_scope_path(mon_base_url):
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&path=%2FOther'
        _assert_status(mon_base_url, path, 'database@builtin', 400)


# Only params that bypass regular path validation (e.g. path_id, schemeshard_id) are forbidden.
def test_viewer_describe_strict_database_token_extra_params(mon_base_url):
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&merge=true'
        _assert_not_status(mon_base_url, path, 'database@builtin', 403)
        _assert_not_status(mon_base_url, path, 'root@builtin', 403)


# path_id bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_strict_database_token_forbidden_params(mon_base_url):
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&path_id=1'
        _assert_status(mon_base_url, path, 'database@builtin', 400)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(mon_base_url, path, 'root@builtin', 403)


# Path outside database scope gives endpoint validation error (400), not role-denied (403).
def test_out_of_scope_path_nodes_gives_400(mon_base_url):
    db_qs = DATABASE.replace("/", "%2F")

    for ep in ['/viewer/nodes', '/viewer/json/nodes']:
        path = f'{ep}?database={db_qs}&path=%2FOther'
        _assert_status(mon_base_url, path, 'database@builtin', 400)
        _assert_not_status(mon_base_url, path, 'database@builtin', 403)


# schemeshard_id bypasses regular path validation, so handler validation rejects it as a bad request.
def test_viewer_describe_schemeshard_id_forbidden(mon_base_url):
    db_qs = DATABASE.replace("/", "%2F")
    for ep in ['/viewer/describe', '/viewer/json/describe']:
        path = f'{ep}?database={db_qs}&schemeshard_id=1'
        _assert_status(mon_base_url, path, 'database@builtin', 400)
        # Non-database-scoped tokens must not be blocked by this check.
        _assert_not_status(mon_base_url, path, 'root@builtin', 403)
