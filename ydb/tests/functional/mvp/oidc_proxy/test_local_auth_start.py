from urllib.parse import parse_qs, urlparse

from oidc_proxy_testlib import (
    assert_redirect_target,
    authenticate_session,
    finish_auth_callback,
    protected_host,
)


LANDING_DATA_PATH = "/tablets/app?TabletID=72057594037968897&page=LandingData"


def test_navigation_request_redirects_to_local_auth_start(oidc_proxy_full_flow_local_auth_start_env):
    env = oidc_proxy_full_flow_local_auth_start_env
    start_path = f"/{protected_host(env)}{LANDING_DATA_PATH}"

    response = env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert response.status_code == 302, response.text
    redirect_url = response.headers["Location"]
    parsed_redirect = urlparse(redirect_url)
    assert parsed_redirect.scheme == "", redirect_url
    assert parsed_redirect.path == "/auth/start", redirect_url
    assert parse_qs(parsed_redirect.query)["return_to"] == [start_path], redirect_url


def test_full_authorization_flow_with_local_auth_start(oidc_proxy_full_flow_local_auth_start_env):
    env = oidc_proxy_full_flow_local_auth_start_env
    start_path = f"/{protected_host(env)}{LANDING_DATA_PATH}"

    session_cookie = authenticate_session(env, start_path)

    assert "__Host_session_cookie_" in session_cookie, session_cookie


def start_navigation_auth_flow_with_local_auth_start(env, start_path, shared_oidc_cookie=None):
    start_response = env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert start_response.status_code == 302, start_response.text
    start_redirect_url = start_response.headers["Location"]
    parsed_start_redirect = urlparse(start_redirect_url)
    assert parsed_start_redirect.path == "/auth/start", start_redirect_url
    assert parse_qs(parsed_start_redirect.query)["return_to"] == [start_path], start_redirect_url

    auth_start_response = env.get(
        start_redirect_url,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            **({"Cookie": shared_oidc_cookie} if shared_oidc_cookie is not None else {}),
        },
    )

    assert auth_start_response.status_code == 302, auth_start_response.text
    assert "Set-Cookie" in auth_start_response.headers, auth_start_response.headers

    redirect_url = auth_start_response.headers["Location"]
    state = parse_qs(urlparse(redirect_url).query)["state"][0]
    next_shared_oidc_cookie = auth_start_response.headers["Set-Cookie"].split(";", 1)[0]
    return state, next_shared_oidc_cookie


def test_callback_uses_entry_from_state_with_local_auth_start(oidc_proxy_full_flow_local_auth_start_env):
    env = oidc_proxy_full_flow_local_auth_start_env
    host = protected_host(env)
    first_target = f"/{host}{LANDING_DATA_PATH}&from=first"
    second_target = f"/{host}{LANDING_DATA_PATH}&from=second"

    first_state, first_shared_oidc_cookie = start_navigation_auth_flow_with_local_auth_start(env, first_target)
    _, second_shared_oidc_cookie = start_navigation_auth_flow_with_local_auth_start(env, second_target, first_shared_oidc_cookie)

    assert first_shared_oidc_cookie.split("=", 1)[0] == second_shared_oidc_cookie.split("=", 1)[0]

    callback_response = finish_auth_callback(
        env,
        first_state,
        [second_shared_oidc_cookie],
    )

    assert_redirect_target(callback_response, first_target)
