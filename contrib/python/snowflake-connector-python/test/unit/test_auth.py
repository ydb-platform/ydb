#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import inspect
import time
from unittest.mock import MagicMock, Mock, PropertyMock

import pytest

from snowflake.connector.constants import OCSPMode
from snowflake.connector.description import CLIENT_NAME, CLIENT_VERSION
from snowflake.connector.network import SnowflakeRestful

try:  # pragma: no cover
    from snowflake.connector.auth import Auth, AuthByDefault, AuthByPlugin
except ImportError:
    from snowflake.connector.auth import Auth
    from snowflake.connector.auth_by_plugin import AuthByPlugin
    from snowflake.connector.auth_default import AuthByDefault


def _init_rest(application, post_requset):
    connection = MagicMock()
    connection._login_timeout = 120
    connection.login_timeout = 120
    connection._network_timeout = None
    connection.errorhandler = Mock(return_value=None)
    connection._ocsp_mode = Mock(return_value=OCSPMode.FAIL_OPEN)
    type(connection).application = PropertyMock(return_value=application)
    type(connection)._internal_application_name = PropertyMock(return_value=CLIENT_NAME)
    type(connection)._internal_application_version = PropertyMock(
        return_value=CLIENT_VERSION
    )

    rest = SnowflakeRestful(
        host="testaccount.snowflakecomputing.com", port=443, connection=connection
    )
    rest._post_request = post_requset
    return rest


def _create_mock_auth_mfs_rest_response(next_action: str):
    def _mock_auth_mfa_rest_response(url, headers, body, **kwargs):
        """Tests successful case."""
        global mock_cnt
        _ = url
        _ = headers
        _ = body
        _ = kwargs.get("dummy")
        if mock_cnt == 0:
            ret = {
                "success": True,
                "message": None,
                "data": {
                    "nextAction": next_action,
                    "inFlightCtx": "inFlightCtx",
                },
            }
        elif mock_cnt == 1:
            ret = {
                "success": True,
                "message": None,
                "data": {
                    "token": "TOKEN",
                    "masterToken": "MASTER_TOKEN",
                },
            }

        mock_cnt += 1
        return ret

    return _mock_auth_mfa_rest_response


def _mock_auth_mfa_rest_response_failure(url, headers, body, **kwargs):
    """Tests failed case."""
    global mock_cnt
    _ = url
    _ = headers
    _ = body
    _ = kwargs.get("dummy")

    if mock_cnt == 0:
        ret = {
            "success": True,
            "message": None,
            "data": {
                "nextAction": "EXT_AUTHN_DUO_ALL",
                "inFlightCtx": "inFlightCtx",
            },
        }
    elif mock_cnt == 1:
        ret = {
            "success": True,
            "message": None,
            "data": {
                "nextAction": "BAD",
                "inFlightCtx": "inFlightCtx",
            },
        }

    mock_cnt += 1
    return ret


def _mock_auth_mfa_rest_response_timeout(url, headers, body, **kwargs):
    """Tests timeout case."""
    global mock_cnt
    _ = url
    _ = headers
    _ = body
    _ = kwargs.get("dummy")
    if mock_cnt == 0:
        ret = {
            "success": True,
            "message": None,
            "data": {
                "nextAction": "EXT_AUTHN_DUO_ALL",
                "inFlightCtx": "inFlightCtx",
            },
        }
    elif mock_cnt == 1:
        time.sleep(10)  # should timeout while here
        ret = {}

    mock_cnt += 1
    return ret


@pytest.mark.parametrize(
    "next_action", ("EXT_AUTHN_DUO_ALL", "EXT_AUTHN_DUO_PUSH_N_PASSCODE")
)
def test_auth_mfa(next_action: str):
    """Authentication by MFA."""
    global mock_cnt
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    password = "testpassword"

    # success test case
    mock_cnt = 0
    rest = _init_rest(application, _create_mock_auth_mfs_rest_response(next_action))
    auth = Auth(rest)
    auth_instance = AuthByDefault(password)
    auth.authenticate(auth_instance, account, user)
    assert not rest._connection.errorhandler.called  # not error
    assert rest.token == "TOKEN"
    assert rest.master_token == "MASTER_TOKEN"

    # failure test case
    mock_cnt = 0
    rest = _init_rest(application, _mock_auth_mfa_rest_response_failure)
    auth = Auth(rest)
    auth_instance = AuthByDefault(password)
    auth.authenticate(auth_instance, account, user)
    assert rest._connection.errorhandler.called  # error

    # timeout 1 second
    mock_cnt = 0
    rest = _init_rest(application, _mock_auth_mfa_rest_response_timeout)
    auth = Auth(rest)
    auth_instance = AuthByDefault(password)
    auth.authenticate(auth_instance, account, user, timeout=1)
    assert rest._connection.errorhandler.called  # error


def _mock_auth_password_change_rest_response(url, headers, body, **kwargs):
    """Test successful case."""
    global mock_cnt
    _ = url
    _ = headers
    _ = body
    _ = kwargs.get("dummy")
    if mock_cnt == 0:
        ret = {
            "success": True,
            "message": None,
            "data": {
                "nextAction": "PWD_CHANGE",
                "inFlightCtx": "inFlightCtx",
            },
        }
    elif mock_cnt == 1:
        ret = {
            "success": True,
            "message": None,
            "data": {
                "token": "TOKEN",
                "masterToken": "MASTER_TOKEN",
            },
        }

    mock_cnt += 1
    return ret


def test_auth_password_change():
    """Tests password change."""
    global mock_cnt

    def _password_callback():
        return "NEW_PASSWORD"

    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    password = "testpassword"

    # success test case
    mock_cnt = 0
    rest = _init_rest(application, _mock_auth_password_change_rest_response)
    auth = Auth(rest)
    auth_instance = AuthByDefault(password)
    auth.authenticate(
        auth_instance, account, user, password_callback=_password_callback
    )
    assert not rest._connection.errorhandler.called  # not error


def test_authbyplugin_abc_api():
    """This test verifies that the abstract function signatures have not changed."""
    bc = AuthByPlugin

    # Verify properties
    assert inspect.isdatadescriptor(bc.timeout)
    assert inspect.isdatadescriptor(bc.type_)
    assert inspect.isdatadescriptor(bc.assertion_content)

    # Verify method signatures
    # update_body
    assert inspect.isfunction(bc.update_body)
    assert str(inspect.signature(bc.update_body).parameters) == (
        "OrderedDict({'self': <Parameter \"self\">, "
        "'body': <Parameter \"body: 'dict[Any, Any]'\">})"
    )

    # authenticate
    assert inspect.isfunction(bc.prepare)
    assert str(inspect.signature(bc.prepare).parameters) == (
        "OrderedDict({'self': <Parameter \"self\">, "
        "'conn': <Parameter \"conn: 'SnowflakeConnection'\">, "
        "'authenticator': <Parameter \"authenticator: 'str'\">, "
        "'service_name': <Parameter \"service_name: 'str | None'\">, "
        "'account': <Parameter \"account: 'str'\">, "
        "'user': <Parameter \"user: 'str'\">, "
        "'password': <Parameter \"password: 'str | None'\">, "
        "'kwargs': <Parameter \"**kwargs: 'Any'\">})"
    )

    # handle_failure
    assert inspect.isfunction(bc._handle_failure)
    assert str(inspect.signature(bc._handle_failure).parameters) == (
        "OrderedDict({'self': <Parameter \"self\">, "
        "'conn': <Parameter \"conn: 'SnowflakeConnection'\">, "
        "'ret': <Parameter \"ret: 'dict[Any, Any]'\">, "
        "'kwargs': <Parameter \"**kwargs: 'Any'\">})"
    )

    # handle_timeout
    assert inspect.isfunction(bc.handle_timeout)
    assert str(inspect.signature(bc.handle_timeout).parameters) == (
        "OrderedDict({'self': <Parameter \"self\">, "
        "'authenticator': <Parameter \"authenticator: 'str'\">, "
        "'service_name': <Parameter \"service_name: 'str | None'\">, "
        "'account': <Parameter \"account: 'str'\">, "
        "'user': <Parameter \"user: 'str'\">, "
        "'password': <Parameter \"password: 'str'\">, "
        "'kwargs': <Parameter \"**kwargs: 'Any'\">})"
    )
