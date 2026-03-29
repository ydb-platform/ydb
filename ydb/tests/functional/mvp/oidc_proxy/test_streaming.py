import pytest
from urllib3.exceptions import ProtocolError

from oidc_proxy_testlib import (
    assert_nc_exchange_requests,
    assert_multipart_counter_sequence,
    authenticate_session,
    get_multipart_stream_with_session,
    protected_host,
    read_stream_body,
    read_first_counter_payload,
)


def multipart_counter_path(host, max_counter=3, period=1, content_type="application/json", fail_chance=0):
    path = (
        f"/{host}/viewer/multipart_counter"
        f"?max_counter={max_counter}"
        f"&period={period}"
        f"&content_type={content_type}"
    )
    if fail_chance:
        path += f"&fail_chance={fail_chance}"
    return path


def start_authenticated_multipart_counter_stream(
    env,
    max_counter=3,
    period=1,
    content_type="application/json",
    fail_chance=0,
):
    host = protected_host(env)
    path = multipart_counter_path(
        host,
        max_counter=max_counter,
        period=period,
        content_type=content_type,
        fail_chance=fail_chance,
    )
    session_cookie = authenticate_session(env, path)
    response = get_multipart_stream_with_session(
        env,
        path,
        session_cookie,
    )
    assert_nc_exchange_requests(env.auth_service)
    return path, session_cookie, response


def start_authenticated_multipart_counter_response(
    env,
    max_counter=3,
    period=1,
    content_type="application/json",
    fail_chance=0,
):
    response = start_authenticated_multipart_counter_stream(
        env,
        max_counter=max_counter,
        period=period,
        content_type=content_type,
        fail_chance=fail_chance,
    )[2]
    return response


def start_authenticated_multipart_counter_stream_with_session(
    env,
    max_counter=3,
    period=1,
    content_type="application/json",
    fail_chance=0,
):
    authenticated_stream = start_authenticated_multipart_counter_stream(
        env,
        max_counter=max_counter,
        period=period,
        content_type=content_type,
        fail_chance=fail_chance,
    )
    session_cookie = authenticated_stream[1]
    response = authenticated_stream[2]
    return session_cookie, response


def test_full_authorization_flow_multipart_counter(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    multipart_response = start_authenticated_multipart_counter_response(env)
    try:
        assert_multipart_counter_sequence(multipart_response, [1, 2, 3])
    finally:
        multipart_response.close()


def test_full_authorization_flow_multipart_counter_client_abort(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    multipart_path, session_cookie, stream_response = start_authenticated_multipart_counter_stream(env)
    read_first_counter_payload(stream_response)
    stream_response.close()

    restarted_stream_response = get_multipart_stream_with_session(
        env,
        multipart_path,
        session_cookie,
    )
    try:
        assert_multipart_counter_sequence(restarted_stream_response, [1, 2, 3])
    finally:
        restarted_stream_response.close()


def test_full_authorization_flow_multipart_counter_server_fail(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    session_cookie, failed_stream_response = start_authenticated_multipart_counter_stream_with_session(
        env,
        fail_chance=100,
    )

    try:
        assert failed_stream_response.status_code == 200, failed_stream_response.text
        with pytest.raises(ProtocolError):
            read_stream_body(failed_stream_response)
    finally:
        failed_stream_response.close()

    host = protected_host(env)
    healthy_multipart_path = multipart_counter_path(host)
    restarted_stream_response = get_multipart_stream_with_session(
        env,
        healthy_multipart_path,
        session_cookie,
    )
    try:
        assert_multipart_counter_sequence(restarted_stream_response, [1, 2, 3])
    finally:
        restarted_stream_response.close()
