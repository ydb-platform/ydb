import json
import re
from base64 import b64encode
from urllib.parse import parse_qs, urlparse


CALLBACK_PATH = "/auth/callback?code=test-code&state=test-state"
TEST_CLIENT_ID = "test-client"
TEST_IAM_TOKEN = "protected_page_iam_token"
TEST_SESSION_TOKEN = "session_token_value"
TEST_IMPERSONATION_TOKEN = "impersonation_token"


def protected_host(env):
    return f"{env.cluster.nodes[1].host}:{env.cluster.nodes[1].mon_port}"


def direct_bearer_headers(token=TEST_IAM_TOKEN):
    return {
        "Host": "oidcproxy.net",
        "Authorization": f"Bearer {token}",
    }


def session_cookie_name(client_id=TEST_CLIENT_ID):
    return f"__Host_session_cookie_{client_id.encode().hex().upper()}"


def impersonated_cookie_name(client_id=TEST_CLIENT_ID):
    return f"__Host_impersonated_cookie_{client_id.encode().hex().upper()}"


def encoded_cookie_value(token):
    return b64encode(token.encode()).decode()


def session_cookie_header(token=TEST_SESSION_TOKEN, client_id=TEST_CLIENT_ID):
    return f"{session_cookie_name(client_id)}={encoded_cookie_value(token)}"


def viewer_query_body(env, query):
    return {
        "action": "execute-query",
        "base64": False,
        "database": f"/{env.cluster.domain_name}",
        "query": query,
        "stats": "full",
        "syntax": "yql_v1",
        "tracingLevel": 9,
    }


def assert_scalar_query_response(response, expected_value):
    assert response.status_code == 200, response.text
    response_json = response.json()
    assert response_json["status"] == "SUCCESS", response_json
    assert len(response_json["result"]) == 1, response_json
    assert response_json["result"][0]["rows"][0][0] == expected_value, response_json


def assert_whoami_response(response, expected_fields):
    assert response.status_code == 200, response.text
    response_json = response.json()
    for field in expected_fields:
        assert field in response_json, response_json


def assert_nc_exchange_requests(auth_service):
    token_request = auth_service.token_requests
    assert len(token_request) == 1
    assert token_request[0]["body"]["code"] == ["code_template#"]
    assert token_request[0]["headers"]["Authorization"] == "OAuth test-service-token"

    exchange_requests = auth_service.exchange_requests
    assert len(exchange_requests) == 1
    assert exchange_requests[0]["body"]["subject_token"] == ["session_token_value"]
    assert exchange_requests[0]["headers"]["Authorization"] == "OAuth test-service-token"


def assert_nc_impersonation_requests(auth_service, service_account_id):
    impersonation_requests = auth_service.impersonation_requests
    assert len(impersonation_requests) == 1
    assert impersonation_requests[0]["body"]["session"] == ["session_token_value"]
    assert impersonation_requests[0]["body"]["service_account_id"] == [service_account_id]
    assert impersonation_requests[0]["headers"]["Authorization"] == "OAuth test-service-token"


def assert_nc_auth_not_started(auth_service):
    assert auth_service.token_requests == []
    assert auth_service.exchange_requests == []
    assert auth_service.impersonation_requests == []


def assert_cookie_is_set(response, expected_status, cookie_marker):
    assert response.status_code == expected_status, response.text
    assert "Set-Cookie" in response.headers, response.headers
    set_cookie = response.headers["Set-Cookie"]
    assert cookie_marker in set_cookie, set_cookie
    return set_cookie


def assert_cookie_is_cleared(response, expected_status, cookie_marker):
    set_cookie = assert_cookie_is_set(response, expected_status, cookie_marker)
    assert "Max-Age=0" in set_cookie


def assert_successful_multipart_counter_response(response):
    assert response.status_code == 200, response.text
    content_type = response.headers["Content-Type"]
    assert content_type.startswith("multipart/x-mixed-replace;boundary=boundary"), content_type
    return content_type


def assert_multipart_counter_sequence(response, expected_counters):
    content_type = assert_successful_multipart_counter_response(response)
    body = read_stream_body(response)
    parts = extract_multipart_json_parts(content_type, body)
    assert [part["Counter"] for part in parts] == expected_counters


def get_auth_callback(env, headers=None, **kwargs):
    return env.get(
        CALLBACK_PATH,
        allow_redirects=False,
        headers=headers,
        **kwargs,
    )


def post_json_with_bearer(env, path, payload, token=TEST_IAM_TOKEN):
    return env.post(
        path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
    )


def get_with_session_cookie(env, path, session_cookie, **kwargs):
    return env.get(
        path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
        },
        **kwargs,
    )


def get_with_bearer(env, path, token=TEST_IAM_TOKEN, **kwargs):
    return env.get(
        path,
        allow_redirects=False,
        headers=direct_bearer_headers(token),
        **kwargs,
    )


def get_multipart_stream_with_session(env, path, session_cookie, timeout=10):
    return env.get(
        path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
            "Accept": "multipart/x-mixed-replace",
        },
        stream=True,
        timeout=timeout,
    )


def authenticate_session(oidc_proxy_full_flow_env, start_path):
    start_response = oidc_proxy_full_flow_env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert start_response.status_code == 302
    assert "Set-Cookie" in start_response.headers
    redirect_url = start_response.headers["Location"]
    parsed_redirect = urlparse(redirect_url)
    assert redirect_url.startswith(oidc_proxy_full_flow_env.auth_service.endpoint + "/oauth/authorize"), redirect_url
    state = parse_qs(parsed_redirect.query)["state"][0]
    oidc_cookie = start_response.headers["Set-Cookie"].split(";", 1)[0]

    callback_response = oidc_proxy_full_flow_env.get(
        f"/auth/callback?code=code_template%23&state={state}",
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": oidc_cookie,
        },
    )

    assert callback_response.status_code == 302, callback_response.text
    assert callback_response.headers["Location"] == start_path, callback_response.headers
    assert "__Host_session_cookie_" in callback_response.headers["Set-Cookie"], callback_response.headers["Set-Cookie"]

    return callback_response.headers["Set-Cookie"].split(";", 1)[0]


def read_stream_body(response):
    chunks = []
    for chunk in response.raw.stream(1024, decode_content=True):
        if chunk:
            chunks.append(chunk)
    body = b"".join(chunks)
    if isinstance(body, bytes):
        return body.decode()
    return body


def extract_multipart_json_parts(content_type, body):
    boundary = content_type.split("boundary=", 1)[1].strip('"')
    assert f"--{boundary}\r\n" in body, body
    assert f"--{boundary}--\r\n" in body, body
    return [json.loads(match) for match in re.findall(r"\{\"Counter\":\s*\d+\}", body)]


def read_first_counter_payload(response):
    first_payload = ""
    for chunk in response.raw.stream(1024, decode_content=True):
        if not chunk:
            continue
        if isinstance(chunk, bytes):
            chunk = chunk.decode()
        first_payload += chunk
        if '"Counter":1' in first_payload or '"Counter": 1' in first_payload:
            break

    assert '"Counter":1' in first_payload or '"Counter": 1' in first_payload, first_payload
    return first_payload
