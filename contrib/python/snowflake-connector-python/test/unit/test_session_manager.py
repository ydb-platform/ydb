#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from enum import Enum
from unittest import mock

from snowflake.connector.network import SnowflakeRestful

try:
    from snowflake.connector.ssl_wrap_socket import DEFAULT_OCSP_MODE
except ImportError:

    class OCSPMode(Enum):
        FAIL_OPEN = "FAIL_OPEN"

    DEFAULT_OCSP_MODE = OCSPMode.FAIL_OPEN

hostname_1 = "sfctest0.snowflakecomputing.com"
url_1 = f"https://{hostname_1}:443/session/v1/login-request"

hostname_2 = "sfc-ds2-customer-stage.s3.amazonaws.com"
url_2 = f"https://{hostname_2}/rgm1-s-sfctest0/stages/"
url_3 = f"https://{hostname_2}/rgm1-s-sfctst0/stages/another-url"


mock_conn = mock.Mock()
mock_conn.disable_request_pooling = False
mock_conn._ocsp_mode = lambda: DEFAULT_OCSP_MODE


def close_sessions(rest: SnowflakeRestful, num_session_pools: int) -> None:
    """Helper function to call SnowflakeRestful.close(). Asserts close was called on all SessionPools."""
    with mock.patch("snowflake.connector.network.SessionPool.close") as close_mock:
        rest.close()
        assert close_mock.call_count == num_session_pools


def create_session(
    rest: SnowflakeRestful, num_sessions: int = 1, url: str | None = None
) -> None:
    """
    Creates 'num_sessions' sessions to 'url'. This is recursive so that idle sessions
    are not reused.
    """
    if num_sessions == 0:
        return
    with rest._use_requests_session(url):
        create_session(rest, num_sessions - 1, url)


@mock.patch("snowflake.connector.network.SnowflakeRestful.make_requests_session")
def test_no_url_multiple_sessions(make_session_mock):
    rest = SnowflakeRestful(connection=mock_conn)

    create_session(rest, 2)

    assert make_session_mock.call_count == 2

    assert list(rest._sessions_map.keys()) == [None]

    session_pool = rest._sessions_map[None]
    assert len(session_pool._idle_sessions) == 2
    assert len(session_pool._active_sessions) == 0

    close_sessions(rest, 1)


@mock.patch("snowflake.connector.network.SnowflakeRestful.make_requests_session")
def test_multiple_urls_multiple_sessions(make_session_mock):
    rest = SnowflakeRestful(connection=mock_conn)

    for url in [url_1, url_2, None]:
        create_session(rest, num_sessions=2, url=url)

    assert make_session_mock.call_count == 6

    hostnames = list(rest._sessions_map.keys())
    for hostname in [hostname_1, hostname_2, None]:
        assert hostname in hostnames

    for pool in rest._sessions_map.values():
        assert len(pool._idle_sessions) == 2
        assert len(pool._active_sessions) == 0

    close_sessions(rest, 3)


@mock.patch("snowflake.connector.network.SnowflakeRestful.make_requests_session")
def test_multiple_urls_reuse_sessions(make_session_mock):
    rest = SnowflakeRestful(connection=mock_conn)
    for url in [url_1, url_2, url_3, None]:
        # create 10 sessions, one after another
        for _ in range(10):
            create_session(rest, url=url)

    # only one session is created and reused thereafter
    assert make_session_mock.call_count == 3

    hostnames = list(rest._sessions_map.keys())
    assert len(hostnames) == 3
    for hostname in [hostname_1, hostname_2, None]:
        assert hostname in hostnames

    for pool in rest._sessions_map.values():
        assert len(pool._idle_sessions) == 1
        assert len(pool._active_sessions) == 0

    close_sessions(rest, 3)
