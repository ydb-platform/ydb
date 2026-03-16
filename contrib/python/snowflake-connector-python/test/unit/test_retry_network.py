#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import errno
import os
import time
from unittest.mock import MagicMock, Mock, PropertyMock

import OpenSSL.SSL
import pytest

from snowflake.connector.compat import (
    BAD_GATEWAY,
    BAD_REQUEST,
    FORBIDDEN,
    GATEWAY_TIMEOUT,
    INTERNAL_SERVER_ERROR,
    OK,
    SERVICE_UNAVAILABLE,
    UNAUTHORIZED,
    BadStatusLine,
    IncompleteRead,
)
from snowflake.connector.errors import (
    DatabaseError,
    InterfaceError,
    OtherHTTPRetryableError,
)
from snowflake.connector.network import (
    STATUS_TO_EXCEPTION,
    RetryRequest,
    SnowflakeRestful,
)

# We need these for our OldDriver tests. We run most up to date tests with the oldest supported driver version
try:
    from snowflake.connector.vendored import requests, urllib3
except ImportError:  # pragma: no cover
    import requests
    import urllib3

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def test_request_exec():
    rest = SnowflakeRestful(host="testaccount.snowflakecomputing.com", port=443)

    default_parameters = {
        "method": "POST",
        "full_url": "https://testaccount.snowflakecomputing.com/",
        "headers": {},
        "data": '{"code": 12345}',
        "token": None,
    }

    # request mock
    output_data = {"success": True, "code": 12345}
    request_mock = MagicMock()
    type(request_mock).status_code = PropertyMock(return_value=OK)
    request_mock.json.return_value = output_data

    # session mock
    session = MagicMock()
    session.request.return_value = request_mock

    # success
    ret = rest._request_exec(session=session, **default_parameters)
    assert ret == output_data, "output data"

    # retryable exceptions
    for errcode in [
        BAD_REQUEST,  # 400
        FORBIDDEN,  # 403
        INTERNAL_SERVER_ERROR,  # 500
        BAD_GATEWAY,  # 502
        SERVICE_UNAVAILABLE,  # 503
        GATEWAY_TIMEOUT,  # 504
        555,  # random 5xx error
    ]:
        type(request_mock).status_code = PropertyMock(return_value=errcode)
        try:
            rest._request_exec(session=session, **default_parameters)
            pytest.fail("should fail")
        except RetryRequest as e:
            cls = STATUS_TO_EXCEPTION.get(errcode, OtherHTTPRetryableError)
            assert isinstance(e.args[0], cls), "must be internal error exception"

    # unauthorized
    type(request_mock).status_code = PropertyMock(return_value=UNAUTHORIZED)
    with pytest.raises(InterfaceError):
        rest._request_exec(session=session, **default_parameters)

    # unauthorized with catch okta unauthorized error
    # TODO: what is the difference to InterfaceError?
    type(request_mock).status_code = PropertyMock(return_value=UNAUTHORIZED)
    with pytest.raises(DatabaseError):
        rest._request_exec(
            session=session, catch_okta_unauthorized_error=True, **default_parameters
        )

    class IncompleteReadMock(IncompleteRead):
        def __init__(self):
            IncompleteRead.__init__(self, "")

    # handle retryable exception
    for exc in [
        requests.exceptions.ConnectTimeout,
        requests.exceptions.ReadTimeout,
        IncompleteReadMock,
        urllib3.exceptions.ProtocolError,
        requests.exceptions.ConnectionError,
        AttributeError,
    ]:
        session = MagicMock()
        session.request = Mock(side_effect=exc)

        try:
            rest._request_exec(session=session, **default_parameters)
            pytest.fail("should fail")
        except RetryRequest as e:
            cause = e.args[0]
            assert isinstance(cause, exc), "same error class"

    # handle OpenSSL errors and BadStateLine
    for exc in [
        OpenSSL.SSL.SysCallError(errno.ECONNRESET),
        OpenSSL.SSL.SysCallError(errno.ETIMEDOUT),
        OpenSSL.SSL.SysCallError(errno.EPIPE),
        OpenSSL.SSL.SysCallError(-1),  # unknown
        # TODO: should we keep this?
        # urllib3.exceptions.ReadTimeoutError(None, None, None),
        BadStatusLine("fake"),
    ]:
        session = MagicMock()
        session.request = Mock(side_effect=exc)
        try:
            rest._request_exec(session=session, **default_parameters)
            pytest.fail("should fail")
        except RetryRequest as e:
            assert e.args[0] == exc, "same error instance"


def test_fetch():
    connection = MagicMock()
    connection.errorhandler = Mock(return_value=None)

    rest = SnowflakeRestful(
        host="testaccount.snowflakecomputing.com", port=443, connection=connection
    )

    class Cnt:
        def __init__(self):
            self.c = 0

        def set(self, cnt):
            self.c = cnt

        def reset(self):
            self.set(0)

    cnt = Cnt()
    default_parameters = {
        "method": "POST",
        "full_url": "https://testaccount.snowflakecomputing.com/",
        "headers": {"cnt": cnt},
        "data": '{"code": 12345}',
    }

    NOT_RETRYABLE = 1000

    class NotRetryableException(Exception):
        pass

    def fake_request_exec(**kwargs):
        headers = kwargs.get("headers")
        cnt = headers["cnt"]
        time.sleep(3)
        if cnt.c <= 1:
            # the first two raises failure
            cnt.c += 1
            raise RetryRequest(Exception("can retry"))
        elif cnt.c == NOT_RETRYABLE:
            # not retryable exception
            raise NotRetryableException("cannot retry")
        else:
            # return success in the third attempt
            return {"success": True, "data": "valid data"}

    # inject a fake method
    rest._request_exec = fake_request_exec

    # first two attempts will fail but third will success
    cnt.reset()
    ret = rest.fetch(timeout=10, **default_parameters)
    assert ret == {"success": True, "data": "valid data"}
    assert not rest._connection.errorhandler.called  # no error

    # first attempt to reach timeout even if the exception is retryable
    cnt.reset()
    ret = rest.fetch(timeout=1, **default_parameters)
    assert ret == {}
    assert rest._connection.errorhandler.called  # error

    # not retryable excpetion
    cnt.set(NOT_RETRYABLE)
    with pytest.raises(NotRetryableException):
        rest.fetch(timeout=7, **default_parameters)

    # first attempt fails and will not retry
    cnt.reset()
    default_parameters["no_retry"] = True
    ret = rest.fetch(timeout=10, **default_parameters)
    assert ret == {}
    assert cnt.c == 1  # failed on first call - did not retry
    assert rest._connection.errorhandler.called  # error
