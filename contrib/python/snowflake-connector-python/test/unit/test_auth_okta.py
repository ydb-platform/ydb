#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from unittest.mock import MagicMock, Mock, PropertyMock

from snowflake.connector.constants import OCSPMode
from snowflake.connector.description import CLIENT_NAME, CLIENT_VERSION
from snowflake.connector.network import SnowflakeRestful

try:  # pragma: no cover
    from snowflake.connector.auth import AuthByOkta
except ImportError:
    from snowflake.connector.auth_okta import AuthByOkta


def test_auth_okta():
    """Authentication by OKTA positive test case."""
    authenticator = "https://testsso.snowflake.net/"
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    password = "testpassword"
    service_name = ""

    ref_sso_url = "https://testsso.snowflake.net/sso"
    ref_token_url = "https://testsso.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url)

    auth = AuthByOkta(application)

    # step 1
    headers, sso_url, token_url = auth._step1(
        rest._connection, authenticator, service_name, account, user
    )
    assert not rest._connection.errorhandler.called  # no error
    assert headers.get("accept") is not None
    assert headers.get("Content-Type") is not None
    assert headers.get("User-Agent") is not None
    assert sso_url == ref_sso_url
    assert token_url == ref_token_url

    # step 2
    auth._step2(rest._connection, authenticator, sso_url, token_url)
    assert not rest._connection.errorhandler.called  # no error

    # step 3
    ref_one_time_token = "1token1"

    def fake_fetch(method, full_url, headers, **kwargs):
        return {
            "cookieToken": ref_one_time_token,
        }

    rest.fetch = fake_fetch
    one_time_token = auth._step3(rest._connection, headers, token_url, user, password)
    assert not rest._connection.errorhandler.called  # no error
    assert one_time_token == ref_one_time_token

    # step 4
    ref_response_html = """
<html><body>
<form action="https://testaccount.snowflakecomputing.com/post_back"></form>
</body></body></html>
"""

    def fake_fetch(method, full_url, headers, **kwargs):
        return ref_response_html

    rest.fetch = fake_fetch
    response_html = auth._step4(rest._connection, one_time_token, sso_url)
    assert response_html == response_html

    # step 5
    rest._protocol = "https"
    rest._host = f"{account}.snowflakecomputing.com"
    rest._port = 443
    auth._step5(rest._connection, ref_response_html)
    assert not rest._connection.errorhandler.called  # no error
    assert ref_response_html == auth._saml_response


def test_auth_okta_step1_negative():
    """Authentication by OKTA step1 negative test case."""
    authenticator = "https://testsso.snowflake.net/"
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    service_name = ""

    # not success status is returned
    ref_sso_url = "https://testsso.snowflake.net/sso"
    ref_token_url = "https://testsso.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url, success=False, message="error")
    auth = AuthByOkta(application)
    # step 1
    _, _, _ = auth._step1(rest._connection, authenticator, service_name, account, user)
    assert rest._connection.errorhandler.called  # error should be raised


def test_auth_okta_step2_negative():
    """Authentication by OKTA step2 negative test case."""
    authenticator = "https://testsso.snowflake.net/"
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    service_name = ""

    # invalid SSO URL
    ref_sso_url = "https://testssoinvalid.snowflake.net/sso"
    ref_token_url = "https://testsso.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url)

    auth = AuthByOkta(application)
    # step 1
    headers, sso_url, token_url = auth._step1(
        rest._connection, authenticator, service_name, account, user
    )
    # step 2
    auth._step2(rest._connection, authenticator, sso_url, token_url)
    assert rest._connection.errorhandler.called  # error

    # invalid TOKEN URL
    ref_sso_url = "https://testsso.snowflake.net/sso"
    ref_token_url = "https://testssoinvalid.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url)

    auth = AuthByOkta(application)
    # step 1
    headers, sso_url, token_url = auth._step1(
        rest._connection, authenticator, service_name, account, user
    )
    # step 2
    auth._step2(rest._connection, authenticator, sso_url, token_url)
    assert rest._connection.errorhandler.called  # error


def test_auth_okta_step3_negative():
    """Authentication by OKTA step3 negative test case."""
    authenticator = "https://testsso.snowflake.net/"
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    password = "testpassword"
    service_name = ""

    ref_sso_url = "https://testsso.snowflake.net/sso"
    ref_token_url = "https://testsso.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url)

    auth = AuthByOkta(application)
    # step 1
    headers, sso_url, token_url = auth._step1(
        rest._connection, authenticator, service_name, account, user
    )
    # step 2
    auth._step2(rest._connection, authenticator, sso_url, token_url)
    assert not rest._connection.errorhandler.called  # no error

    # step 3: authentication by IdP failed.
    def fake_fetch(method, full_url, headers, **kwargs):
        return {
            "failed": "auth failed",
        }

    rest.fetch = fake_fetch
    _ = auth._step3(rest._connection, headers, token_url, user, password)
    assert rest._connection.errorhandler.called  # auth failure error


def test_auth_okta_step5_negative():
    """Authentication by OKTA step5 negative test case."""
    authenticator = "https://testsso.snowflake.net/"
    application = "testapplication"
    account = "testaccount"
    user = "testuser"
    password = "testpassword"
    service_name = ""

    ref_sso_url = "https://testsso.snowflake.net/sso"
    ref_token_url = "https://testsso.snowflake.net/token"
    rest = _init_rest(ref_sso_url, ref_token_url)

    auth = AuthByOkta(application)
    # step 1
    headers, sso_url, token_url = auth._step1(
        rest._connection, authenticator, service_name, account, user
    )
    assert not rest._connection.errorhandler.called  # no error
    # step 2
    auth._step2(rest._connection, authenticator, sso_url, token_url)
    assert not rest._connection.errorhandler.called  # no error
    # step 3
    ref_one_time_token = "1token1"

    def fake_fetch(method, full_url, headers, **kwargs):
        return {
            "cookieToken": ref_one_time_token,
        }

    rest.fetch = fake_fetch
    one_time_token = auth._step3(rest._connection, headers, token_url, user, password)
    assert not rest._connection.errorhandler.called  # no error

    # step 4
    # HTML includes invalid account name
    ref_response_html = """
<html><body>
<form action="https://invalidtestaccount.snowflakecomputing.com/post_back
"></form>
</body></body></html>
"""

    def fake_fetch(method, full_url, headers, **kwargs):
        return ref_response_html

    rest.fetch = fake_fetch
    response_html = auth._step4(rest._connection, one_time_token, sso_url)
    assert response_html == ref_response_html

    # step 5
    rest._protocol = "https"
    rest._host = f"{account}.snowflakecomputing.com"
    rest._port = 443
    auth._step5(rest._connection, ref_response_html)
    assert rest._connection.errorhandler.called  # error


def _init_rest(ref_sso_url, ref_token_url, success=True, message=None):
    def post_request(url, headers, body, **kwargs):
        _ = url
        _ = headers
        _ = body
        _ = kwargs.get("dummy")
        return {
            "success": success,
            "message": message,
            "data": {
                "ssoUrl": ref_sso_url,
                "tokenUrl": ref_token_url,
            },
        }

    connection = MagicMock()
    connection._login_timeout = 120
    connection._network_timeout = None
    connection.errorhandler = Mock(return_value=None)
    connection._ocsp_mode = Mock(return_value=OCSPMode.FAIL_OPEN)
    type(connection).application = PropertyMock(return_value=CLIENT_NAME)
    type(connection)._internal_application_name = PropertyMock(return_value=CLIENT_NAME)
    type(connection)._internal_application_version = PropertyMock(
        return_value=CLIENT_VERSION
    )

    rest = SnowflakeRestful(
        host="testaccount.snowflakecomputing.com", port=443, connection=connection
    )
    connection._rest = rest
    rest._post_request = post_request
    return rest
