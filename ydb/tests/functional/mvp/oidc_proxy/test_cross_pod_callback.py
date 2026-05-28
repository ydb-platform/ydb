from oidc_proxy_testlib import (
    assert_nc_exchange_requests,
    assert_whoami_response,
    authenticate_session,
    finish_auth_callback,
    get_with_session_cookie,
    protected_host,
    start_navigation_auth_flow,
)

WHOAMI_FIELDS = [
    "IsTokenRequired",
    "IsViewerAllowed",
    "IsMonitoringAllowed",
    "IsAdministrationAllowed",
]


def whoami_path(env):
    return f"/{protected_host(env)}/viewer/json/whoami"


def test_cross_pod_callback_restores_auth_flow(oidc_proxy_cross_pod_full_flow_env):
    env = oidc_proxy_cross_pod_full_flow_env
    path = whoami_path(env.owner)

    session_cookie = authenticate_session(env.owner, path, callback_env=env.worker)

    whoami_response = get_with_session_cookie(env.worker, path, session_cookie)
    assert_whoami_response(whoami_response, WHOAMI_FIELDS)
    assert_nc_exchange_requests(env.auth_service)


def test_cross_pod_callback_falls_back_to_cookie_when_owner_unavailable(oidc_proxy_cross_pod_full_flow_env):
    env = oidc_proxy_cross_pod_full_flow_env
    path = whoami_path(env.owner)

    state, oidc_cookie = start_navigation_auth_flow(env.owner, path)
    env.stop_owner()

    callback_response = finish_auth_callback(env.worker, state, [oidc_cookie])
    assert callback_response.status_code == 302, callback_response.text
    assert callback_response.headers["Location"] == path, callback_response.headers
    assert "__Host-session_cookie_" in callback_response.headers["Set-Cookie"], callback_response.headers["Set-Cookie"]

    session_cookie = callback_response.headers["Set-Cookie"].split(";", 1)[0]
    whoami_response = get_with_session_cookie(env.worker, path, session_cookie)
    assert_whoami_response(whoami_response, WHOAMI_FIELDS)
    assert_nc_exchange_requests(env.auth_service)
