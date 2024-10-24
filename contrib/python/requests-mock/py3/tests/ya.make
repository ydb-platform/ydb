PY3TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/mock
    contrib/python/requests-futures
    contrib/python/requests-mock
)

TEST_SRCS(
    base.py
    pytest/__init__.py
    pytest/test_with_pytest.py
    #test_adapter.py - need purl
    test_custom_matchers.py
    #test_fixture.py - need fixtures
    test_matcher.py
    test_mocker.py
    test_request.py
    test_response.py
)

NO_LINT()

END()
