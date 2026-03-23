import json
import re
import requests
from urllib.parse import parse_qs, urlparse


CALLBACK_PATH = "/auth/callback?code=test-code&state=test-state"


def assert_has_request_id_header(headers):
    assert "x-request-id" in headers
    assert headers["x-request-id"]


def authenticate_nebius_session(oidc_proxy_full_flow_env, start_path):
    start_response = oidc_proxy_full_flow_env.get(
        start_path,
        allow_redirects=False,
        headers={"Host": "oidcproxy.net"},
    )

    assert start_response.status_code == 302
    assert_has_request_id_header(start_response.headers)
    assert "Set-Cookie" in start_response.headers
    redirect_url = start_response.headers["Location"]
    parsed_redirect = urlparse(redirect_url)
    assert redirect_url.startswith(oidc_proxy_full_flow_env.auth_service.endpoint + "/oauth/authorize")
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

    assert callback_response.status_code == 302
    assert_has_request_id_header(callback_response.headers)
    assert callback_response.headers["Location"] == start_path
    assert "__Host_session_cookie_" in callback_response.headers["Set-Cookie"]

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
    assert f"--{boundary}\r\n" in body
    assert f"--{boundary}--\r\n" in body
    return [json.loads(match) for match in re.findall(r"\{\"Counter\":\s*\d+\}", body)]


def test_auth_callback_without_cookie_returns_bad_request_without_hanging(oidc_proxy_env):
    response = oidc_proxy_env.get(
        CALLBACK_PATH,
        allow_redirects=False,
    )

    assert response.status_code == 400
    assert_has_request_id_header(response.headers)


def test_auth_callback_echoes_incoming_request_id(oidc_proxy_env):
    expected_request_id = "9b8c9e5c-1a7d-4f0b-9c3e-8f2a1b2c3d4e"

    response = oidc_proxy_env.get(
        CALLBACK_PATH,
        allow_redirects=False,
        headers={"x-request-id": expected_request_id},
    )

    assert response.status_code == 400
    assert response.headers["x-request-id"] == expected_request_id


def test_auth_callback_without_cookie_with_accept_encoding_returns_bad_request(oidc_proxy_env):
    response = oidc_proxy_env.get(
        CALLBACK_PATH,
        allow_redirects=False,
        headers={"Accept-Encoding": "gzip, deflate"},
    )

    assert response.status_code == 400
    assert_has_request_id_header(response.headers)


def test_auth_callback_on_keep_alive_session_handles_two_requests(oidc_proxy_env):
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

    assert first.status_code == 400
    assert_has_request_id_header(first.headers)
    assert second.status_code == 400
    assert_has_request_id_header(second.headers)
    assert first.headers["x-request-id"] != second.headers["x-request-id"]


def test_auth_callback_echoes_uppercase_request_id_header(oidc_proxy_env):
    expected_request_id = "6f7e8d9c-0a1b-4c2d-8e3f-4a5b6c7d8e9f"

    response = oidc_proxy_env.get(
        CALLBACK_PATH,
        allow_redirects=False,
        headers={"X-Request-Id": expected_request_id},
    )

    assert response.status_code == 400
    assert response.headers["x-request-id"] == expected_request_id


def test_full_authorization_flow_nebius_viewer_query(oidc_proxy_full_flow_env):
    protected_host = f"{oidc_proxy_full_flow_env.cluster.nodes[1].host}:{oidc_proxy_full_flow_env.cluster.nodes[1].mon_port}"
    start_path = f"/{protected_host}/viewer"
    query_path = f"/{protected_host}/viewer/json/query?schema=multi&base64=false"
    query_body = {
        "action": "execute-query",
        "base64": False,
        "database": f"/{oidc_proxy_full_flow_env.cluster.domain_name}",
        "query": "select 1;",
        "stats": "full",
        "syntax": "yql_v1",
        "tracingLevel": 9,
    }

    session_cookie = authenticate_nebius_session(oidc_proxy_full_flow_env, start_path)

    protected_response = oidc_proxy_full_flow_env.post(
        query_path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
            "Content-Type": "application/json",
        },
        data=json.dumps(query_body),
    )

    assert protected_response.status_code == 200
    assert_has_request_id_header(protected_response.headers)
    response_json = protected_response.json()
    assert response_json["status"] == "SUCCESS"
    assert len(response_json["result"]) == 1
    assert response_json["result"][0]["rows"][0][0] == 1

    token_request = oidc_proxy_full_flow_env.auth_service.server.token_requests
    assert len(token_request) == 1
    assert token_request[0]["body"]["code"] == ["code_template#"]
    assert token_request[0]["headers"]["Authorization"] == "OAuth test-service-token"

    exchange_requests = oidc_proxy_full_flow_env.auth_service.server.exchange_requests
    assert len(exchange_requests) == 1
    assert exchange_requests[0]["body"]["subject_token"] == ["session_token_value"]
    assert exchange_requests[0]["headers"]["Authorization"] == "OAuth test-service-token"


def test_full_authorization_flow_nebius_multipart_counter(oidc_proxy_full_flow_env):
    protected_host = f"{oidc_proxy_full_flow_env.cluster.nodes[1].host}:{oidc_proxy_full_flow_env.cluster.nodes[1].mon_port}"
    multipart_path = f"/{protected_host}/viewer/multipart_counter?max_counter=3&period=1&content_type=application/json"
    session_cookie = authenticate_nebius_session(oidc_proxy_full_flow_env, multipart_path)

    response = oidc_proxy_full_flow_env.get(
        multipart_path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
            "Accept": "multipart/x-mixed-replace",
        },
        stream=True,
        timeout=10,
    )

    assert response.status_code == 200
    assert_has_request_id_header(response.headers)
    content_type = response.headers["Content-Type"]
    assert content_type.startswith("multipart/x-mixed-replace;boundary=boundary")

    body = read_stream_body(response)
    parts = extract_multipart_json_parts(content_type, body)
    assert [part["Counter"] for part in parts] == [1, 2, 3]


def test_full_authorization_flow_nebius_multipart_counter_client_abort(oidc_proxy_full_flow_env):
    protected_host = f"{oidc_proxy_full_flow_env.cluster.nodes[1].host}:{oidc_proxy_full_flow_env.cluster.nodes[1].mon_port}"
    multipart_path = f"/{protected_host}/viewer/multipart_counter?max_counter=100&period=10&content_type=application/json"
    query_path = f"/{protected_host}/viewer/json/query?schema=multi&base64=false"
    query_body = {
        "action": "execute-query",
        "base64": False,
        "database": f"/{oidc_proxy_full_flow_env.cluster.domain_name}",
        "query": "select 1;",
        "stats": "full",
        "syntax": "yql_v1",
        "tracingLevel": 9,
    }
    session_cookie = authenticate_nebius_session(oidc_proxy_full_flow_env, multipart_path)

    stream_response = oidc_proxy_full_flow_env.get(
        multipart_path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
            "Accept": "multipart/x-mixed-replace",
        },
        stream=True,
        timeout=10,
    )
    assert stream_response.status_code == 200
    assert_has_request_id_header(stream_response.headers)

    first_payload = ""
    for chunk in stream_response.raw.stream(1024, decode_content=True):
        if not chunk:
            continue
        if isinstance(chunk, bytes):
            chunk = chunk.decode()
        first_payload += chunk
        if '"Counter":1' in first_payload or '"Counter": 1' in first_payload:
            break

    assert '"Counter":1' in first_payload or '"Counter": 1' in first_payload
    stream_response.close()

    query_response = oidc_proxy_full_flow_env.post(
        query_path,
        allow_redirects=False,
        headers={
            "Host": "oidcproxy.net",
            "Cookie": session_cookie,
            "Content-Type": "application/json",
        },
        data=json.dumps(query_body),
    )

    assert query_response.status_code == 200
    assert_has_request_id_header(query_response.headers)
    response_json = query_response.json()
    assert response_json["status"] == "SUCCESS"
    assert response_json["result"][0]["rows"][0][0] == 1
