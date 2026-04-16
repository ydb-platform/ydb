from oidc_proxy_testlib import (
    assert_cookie_is_cleared,
    assert_cookie_is_set,
    assert_nc_impersonation_requests,
    assert_scalar_query_response,
    assert_whoami_response,
    get_with_bearer,
    get_with_session_cookie,
    get_with_session_cookie_headers,
    impersonated_cookie_name,
    post_json_with_bearer,
    protected_host,
    session_cookie_header,
    session_cookie_name,
    viewer_query_body,
)


def test_viewer_query_with_direct_iam_token(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    query_path = f"/{host}/viewer/json/query?schema=multi&base64=false"
    query = "select 1;"
    expected_value = 1
    query_body = viewer_query_body(env, query)

    protected_response = post_json_with_bearer(
        env,
        query_path,
        query_body,
    )

    assert_scalar_query_response(protected_response, expected_value)


def test_viewer_whoami_with_direct_iam_token(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    whoami_path = f"/{host}/viewer/json/whoami"
    expected_fields = [
        "IsTokenRequired",
        "IsViewerAllowed",
        "IsMonitoringAllowed",
        "IsAdministrationAllowed",
    ]

    whoami_response = get_with_bearer(env, whoami_path)

    assert_whoami_response(whoami_response, expected_fields)


def test_impersonation_start_stop_with_session_cookie(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    service_account_id = "serviceaccount-e0tydb-dev"
    expected_status = 200
    expected_cookie_marker = impersonated_cookie_name()
    session_cookie = session_cookie_header()

    impersonate_start_response = get_with_session_cookie(
        env,
        f"/impersonate/start?service_account_id={service_account_id}",
        session_cookie,
    )

    impersonated_cookie = assert_cookie_is_set(
        impersonate_start_response,
        expected_status,
        expected_cookie_marker,
    ).split(";", 1)[0]
    assert_nc_impersonation_requests(env.auth_service, service_account_id)

    impersonate_stop_response = get_with_session_cookie(
        env,
        "/impersonate/stop",
        impersonated_cookie,
    )

    assert_cookie_is_cleared(impersonate_stop_response, expected_status, expected_cookie_marker)


def test_cleanup_clears_session_cookie(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    expected_status = 200
    expected_cookie_marker = session_cookie_name()
    session_cookie = session_cookie_header()

    cleanup_response = get_with_session_cookie(env, "/auth/cleanup", session_cookie)

    assert_cookie_is_cleared(cleanup_response, expected_status, expected_cookie_marker)


def test_expired_session_cookie_fetch_request_returns_json_401(oidc_proxy_full_flow_env):
    env = oidc_proxy_full_flow_env
    host = protected_host(env)
    protected_path = f"/{host}/viewer/json/whoami"
    referer_path = f"/{host}/tablets/app?TabletID=1&page=LandingData&nodes=1&moves=1"
    session_cookie = session_cookie_header()
    env.auth_service.exchange_errors_by_subject_token["session_token_value"] = 401

    response = get_with_session_cookie_headers(
        env,
        protected_path,
        session_cookie,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": f"https://oidcproxy.net{referer_path}",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        },
    )

    assert response.status_code == 401, response.text
    response_json = response.json()
    assert response_json["error"] == "Authorization Required", response_json
    assert "authUrl" in response_json, response_json
    assert "Location" not in response.headers, response.headers
    assert len(env.auth_service.exchange_requests) == 1, env.auth_service.exchange_requests
