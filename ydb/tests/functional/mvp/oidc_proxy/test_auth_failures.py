import requests
from urllib.parse import parse_qs, urlparse

from oidc_proxy_testlib import (
    CALLBACK_PATH,
    assert_nc_auth_not_started,
    protected_host,
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


def test_auth_callback_without_oidc_cookie_restores_requested_page_from_state(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    start_path = f"/{protected_host(env)}/tablets/app?TabletID=72057594037968897&page=LandingData"

    start_response = env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert start_response.status_code == 302, start_response.text
    redirect_url = start_response.headers["Location"]
    parsed_redirect = urlparse(redirect_url)
    state = parse_qs(parsed_redirect.query)["state"][0]

    callback_response = env.get(
        f"/auth/callback?code=code_template%23&state={state}",
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert callback_response.status_code == 302, callback_response.text
    assert callback_response.headers["Location"] == start_path, callback_response.headers
    assert "__Host_session_cookie_" in callback_response.headers["Set-Cookie"], callback_response.headers
