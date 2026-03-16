from graphql_server import HttpQueryError


def test_can_create_http_query_error():
    error = HttpQueryError(400, "Bad error")
    assert error.status_code == 400
    assert error.message == "Bad error"
    assert not error.is_graphql_error
    assert error.headers is None


def test_compare_http_query_errors():
    error = HttpQueryError(400, "Bad error")
    assert error == error
    same_error = HttpQueryError(400, "Bad error")
    assert error == same_error
    different_error = HttpQueryError(400, "Not really bad error")
    assert error != different_error
    different_error = HttpQueryError(405, "Bad error")
    assert error != different_error
    different_error = HttpQueryError(400, "Bad error", headers={"Allow": "ALL"})
    assert error != different_error


def test_hash_http_query_errors():
    errors = {
        HttpQueryError(400, "Bad error 1"),
        HttpQueryError(400, "Bad error 2"),
        HttpQueryError(403, "Bad error 1"),
    }
    assert HttpQueryError(400, "Bad error 1") in errors
    assert HttpQueryError(400, "Bad error 2") in errors
    assert HttpQueryError(403, "Bad error 1") in errors
    assert HttpQueryError(403, "Bad error 2") not in errors
