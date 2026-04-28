import requests
from urllib.parse import urlparse

from oidc_proxy_testlib import (
    CALLBACK_PATH,
    assert_nc_auth_not_started,
    get_auth_callback,
    protected_host,
    session_cookie_header,
)

AUTH_AUTHORIZE_PATH = "/oauth/authorize"
LANDING_DATA_PATH = "/tablets/app?TabletID=72057594037968897&page=LandingData"


def assert_bad_auth_callback_response(response):
    assert response.status_code == 400
    assert "Set-Cookie" not in response.headers, response.headers
    assert "Location" not in response.headers, response.headers


def test_auth_callback_without_request_id_is_bad_request(oidc_proxy_env):
    response = get_auth_callback(oidc_proxy_env)
    assert_bad_auth_callback_response(response)
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
    assert_bad_auth_callback_response(first)
    assert_bad_auth_callback_response(second)
    assert_nc_auth_not_started(oidc_proxy_env.auth_service)


def build_base_expired_session_request_headers(host):
    headers = {
        "Host": "oidcproxy.net",
        "Cookie": session_cookie_header(token="broken-session"),
        "Referer": f"https://oidcproxy.net/{host}/tablets/app?TabletID=72057594037968897",
        "Sec-Fetch-Site": "same-origin",
    }
    return headers


def build_ajax_expired_session_request_headers(host):
    headers = build_base_expired_session_request_headers(host)
    headers.update({
        "Accept": "*/*",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "X-Requested-With": "XMLHttpRequest",
    })
    return headers


def build_navigation_expired_session_request_headers(host):
    headers = build_base_expired_session_request_headers(host)
    headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
    })
    return headers


def start_auth_challenge_request(env, headers):
    env.auth_service.exchange_errors_by_subject_token["broken-session"] = 401

    return env.get(
        get_protected_landing_data_path(env),
        allow_redirects=False,
        headers=headers,
    )


def get_protected_landing_data_path(env):
    return f"/{protected_host(env)}{LANDING_DATA_PATH}"


def start_ajax_auth_challenge_request(env):
    host = protected_host(env)
    return start_auth_challenge_request(
        env,
        build_ajax_expired_session_request_headers(host),
    )


def start_navigation_auth_challenge_request(env):
    host = protected_host(env)
    return start_auth_challenge_request(
        env,
        build_navigation_expired_session_request_headers(host),
    )


def assert_ajax_auth_challenge(response):
    assert response.status_code == 401, response.text
    response_json = response.json()
    assert response_json["error"] == "Authorization Required", response_json
    assert urlparse(response_json["authUrl"]).path == AUTH_AUTHORIZE_PATH, response_json
    assert "Location" not in response.headers, response.headers
    assert "Set-Cookie" in response.headers, response.headers


def assert_navigation_auth_redirect(env, response):
    assert response.status_code == 302, response.text
    redirect_url = response.headers["Location"]
    assert redirect_url.startswith(env.auth_service.endpoint + AUTH_AUTHORIZE_PATH), redirect_url


def test_ajax_request_returns_json_401_with_auth_url(oidc_proxy_full_flow_env):
    response = start_ajax_auth_challenge_request(oidc_proxy_full_flow_env)
    assert_ajax_auth_challenge(response)


def test_navigation_request_returns_oidc_redirect(oidc_proxy_full_flow_env):
    response = start_navigation_auth_challenge_request(oidc_proxy_full_flow_env)
    assert_navigation_auth_redirect(oidc_proxy_full_flow_env, response)
