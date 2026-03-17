PY3TEST()

PEERDIR(
    contrib/python/pytest-flask
)

TEST_SRCS(
    conftest.py
    test_fixtures.py
    test_internal.py
    test_json_response.py
    #test_live_server.py
    test_markers.py
    test_response_overwriting.py
)

NO_LINT()

END()
