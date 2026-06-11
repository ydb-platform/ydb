import requests
from urllib.parse import parse_qs, urlparse

from oidc_proxy_testlib import (
    CALLBACK_PATH,
    assert_nc_auth_not_started,
    auth_url_from_response,
    decode_oidc_state,
    get_auth_callback,
    protected_host,
    response_cookie_by_name,
    session_cookie_header,
)

LANDING_DATA_PATH = "/tablets/app?TabletID=72057594037968897&page=LandingData"
AUTH_AUTHORIZE_PATH = "/oauth/authorize"
BROKEN_SESSION_TOKEN = "broken-session"
BACKGROUND_FETCH_HEADERS = {
    "Accept": "*/*",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "X-Requested-With": "XMLHttpRequest",
}
NAVIGATION_FETCH_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-User": "?1",
}


def assert_bad_auth_callback_response(response):
    assert response.status_code == 400
    assert "Set-Cookie" not in response.headers, response.headers
    assert "Location" not in response.headers, response.headers


def tamper_cookie_value(cookie_pair):
    cookie_name, cookie_value = cookie_pair.split("=", 1)
    tampered_first_char = "b" if cookie_value[0] == "a" else "a"
    return f"{cookie_name}={tampered_first_char}{cookie_value[1:]}"


def oidc_authorize_url_prefix(env):
    return env.auth_service.endpoint + AUTH_AUTHORIZE_PATH


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


def build_expired_session_request_headers(host, extra_headers, referer=None):
    headers = {
        "Host": "oidcproxy.net",
        "Cookie": session_cookie_header(token=BROKEN_SESSION_TOKEN),
        "Referer": referer or f"https://oidcproxy.net/{host}/tablets/app?TabletID=72057594037968897",
        "Sec-Fetch-Site": "same-origin",
    }
    headers.update(extra_headers)
    return headers


def build_background_expired_session_request_headers(host):
    return build_expired_session_request_headers(host, BACKGROUND_FETCH_HEADERS)


def build_navigation_expired_session_request_headers(host):
    return build_expired_session_request_headers(host, NAVIGATION_FETCH_HEADERS)


def start_auth_challenge_request(env, headers):
    env.auth_service.exchange_errors_by_subject_token["broken-session"] = 401

    return env.get(
        get_protected_landing_data_path(env),
        allow_redirects=False,
        headers=headers,
    )


def get_protected_landing_data_path(env):
    return f"/{protected_host(env)}{LANDING_DATA_PATH}"


def start_background_auth_challenge_request(env):
    host = protected_host(env)
    return start_auth_challenge_request(
        env,
        build_background_expired_session_request_headers(host),
    )


def start_navigation_auth_challenge_request(env):
    host = protected_host(env)
    return start_auth_challenge_request(
        env,
        build_navigation_expired_session_request_headers(host),
    )


def assert_background_auth_challenge(env, response):
    assert response.status_code == 401, response.text
    response_json = response.json()
    assert response_json["error"] == "Authorization Required", response_json
    assert response_json["authUrl"].startswith(oidc_authorize_url_prefix(env)), response_json
    assert "Location" not in response.headers, response.headers
    assert "Set-Cookie" in response.headers, response.headers


def assert_navigation_auth_redirect(env, response):
    assert response.status_code == 302, response.text
    redirect_url = response.headers["Location"]
    assert redirect_url.startswith(oidc_authorize_url_prefix(env)), redirect_url
    assert "Set-Cookie" in response.headers, response.headers


def extract_auth_flow(response):
    auth_url = auth_url_from_response(response)
    state = parse_qs(urlparse(auth_url).query)["state"][0]
    oidc_cookie = response_cookie_by_name(response, "ydb_oidc_cookie")
    return state, oidc_cookie


def assert_callback_restarts_to(response, target):
    assert response.status_code == 302, response.text
    assert response.headers["Location"] == target, response.headers


def start_navigation_auth_flow(env, start_path):
    response = env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )
    assert_navigation_auth_redirect(env, response)
    return extract_auth_flow(response)


def start_background_auth_flow(env):
    response = start_background_auth_challenge_request(env)
    assert_background_auth_challenge(env, response)
    return extract_auth_flow(response)


def finish_auth_callback(env, state, oidc_cookies=()):
    headers = {"Host": "oidcproxy.net"}
    if oidc_cookies:
        headers["Cookie"] = "; ".join(oidc_cookies)
    return env.get(
        "/auth/callback",
        params={
            "code": "code_template#",
            "state": state,
        },
        allow_redirects=False,
        headers=headers,
    )


def test_background_request_returns_json_401_with_auth_url(oidc_proxy_full_flow_env):
    response = start_background_auth_challenge_request(oidc_proxy_full_flow_env)
    assert_background_auth_challenge(oidc_proxy_full_flow_env, response)


def test_navigation_request_returns_oidc_redirect(oidc_proxy_full_flow_env):
    response = start_navigation_auth_challenge_request(oidc_proxy_full_flow_env)
    assert_navigation_auth_redirect(oidc_proxy_full_flow_env, response)


def test_parallel_auth_attempts_keep_navigation_callback_context(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    navigation_target = f"/{host}{LANDING_DATA_PATH}&from=navigation"

    navigation_state, navigation_oidc_cookie = start_navigation_auth_flow(env, navigation_target)
    background_state, background_oidc_cookie = start_background_auth_flow(env)
    assert navigation_state != background_state

    callback_response = finish_auth_callback(env, navigation_state, [background_oidc_cookie])

    assert_callback_restarts_to(callback_response, navigation_target)


def test_callback_without_matching_auth_flow_cookie_restarts_without_contacting_auth_server(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    navigation_target = f"/{host}{LANDING_DATA_PATH}&from=navigation"

    navigation_state, _ = start_navigation_auth_flow(env, navigation_target)

    callback_response = finish_auth_callback(env, navigation_state)

    assert_callback_restarts_to(callback_response, navigation_target)
    assert_nc_auth_not_started(env.auth_service)


def test_callback_with_invalid_auth_flow_cookie_restarts_without_contacting_auth_server(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    navigation_target = f"/{host}{LANDING_DATA_PATH}&from=navigation"

    navigation_state, navigation_oidc_cookie = start_navigation_auth_flow(env, navigation_target)

    callback_response = finish_auth_callback(env, navigation_state, [tamper_cookie_value(navigation_oidc_cookie)])

    assert_callback_restarts_to(callback_response, navigation_target)
    assert_nc_auth_not_started(env.auth_service)


def test_long_navigation_url_falls_back_to_auth_flow_cookie(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    navigation_target = f"/{host}{LANDING_DATA_PATH}&pad={'a' * 1000}"

    navigation_state, navigation_oidc_cookie = start_navigation_auth_flow(env, navigation_target)
    decoded_state = decode_oidc_state(navigation_state)
    assert "requested_address" not in decoded_state, decoded_state

    callback_response = finish_auth_callback(env, navigation_state, [navigation_oidc_cookie])

    assert_callback_restarts_to(callback_response, navigation_target)


def test_parallel_long_navigation_urls_latest_auth_flow_cookie_fallback_wins(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    first_navigation_target = f"/{host}{LANDING_DATA_PATH}&pad={'a' * 1000}&attempt=first"
    second_navigation_target = f"/{host}{LANDING_DATA_PATH}&pad={'b' * 1000}&attempt=second"

    first_state, first_oidc_cookie = start_navigation_auth_flow(env, first_navigation_target)
    second_state, second_oidc_cookie = start_navigation_auth_flow(env, second_navigation_target)

    assert first_state != second_state
    assert "requested_address" not in decode_oidc_state(first_state)
    assert "requested_address" not in decode_oidc_state(second_state)

    callback_response = finish_auth_callback(env, first_state, [second_oidc_cookie])

    assert_callback_restarts_to(callback_response, second_navigation_target)


def test_background_request_with_requested_address_too_large_for_state_and_cookie_is_bad_request(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    oversized_requested_address = f"https://oidcproxy.net/{host}{LANDING_DATA_PATH}&pad={'a' * 5000}"
    headers = build_expired_session_request_headers(
        host,
        BACKGROUND_FETCH_HEADERS,
        referer=oversized_requested_address,
    )

    response = start_auth_challenge_request(env, headers)

    assert response.status_code == 400, response.text
    assert "Location" not in response.headers, response.headers
    assert "Set-Cookie" not in response.headers, response.headers
    assert "requested address is too large to preserve during authentication" in response.text, response.text
    assert env.auth_service.token_requests == []
