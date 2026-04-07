import requests

from oidc_proxy_testlib import (
    CALLBACK_PATH,
    assert_nc_auth_not_started,
    get_auth_callback,
)


def test_auth_callback_without_request_id_is_bad_request(oidc_proxy_env):
    response = get_auth_callback(oidc_proxy_env)
    assert response.status_code == 400
    assert "Set-Cookie" not in response.headers, response.headers
    assert "Location" not in response.headers, response.headers
    assert_nc_auth_not_started(oidc_proxy_env.auth_service)


def test_auth_callback_on_keep_alive_session_handles_two_requests(oidc_proxy_env):
    expected_status = 400

    with requests.Session() as session:
        first = session.get(
            f"{oidc_proxy_env.endpoint}{CALLBACK_PATH}",
            allow_redirects=False,
            timeout=5,
        )
        second = session.get(
            f"{oidc_proxy_env.endpoint}{CALLBACK_PATH}",
            allow_redirects=False,
            timeout=5,
        )

    assert first.status_code == expected_status
    assert second.status_code == expected_status
    assert "Set-Cookie" not in first.headers, first.headers
    assert "Set-Cookie" not in second.headers, second.headers
    assert "Location" not in first.headers, first.headers
    assert "Location" not in second.headers, second.headers
    assert_nc_auth_not_started(oidc_proxy_env.auth_service)
