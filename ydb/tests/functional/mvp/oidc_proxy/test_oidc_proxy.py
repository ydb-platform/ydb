def assert_has_request_id_header(headers):
    assert "x-request-id" in headers
    assert headers["x-request-id"]


def test_auth_callback_without_cookie_returns_bad_request_without_hanging(oidc_proxy_env):
    response = oidc_proxy_env.get(
        "/auth/callback?code=test-code&state=test-state",
        allow_redirects=False,
    )

    assert response.status_code == 400
    assert_has_request_id_header(response.headers)


def test_auth_callback_echoes_incoming_request_id(oidc_proxy_env):
    expected_request_id = "9b8c9e5c-1a7d-4f0b-9c3e-8f2a1b2c3d4e"

    response = oidc_proxy_env.get(
        "/auth/callback?code=test-code&state=test-state",
        allow_redirects=False,
        headers={"x-request-id": expected_request_id},
    )

    assert response.status_code == 400
    assert response.headers["x-request-id"] == expected_request_id
