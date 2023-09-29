PY2TEST()

PEERDIR(
    contrib/python/pytest-localserver
    contrib/python/responses
)

DATA(
    arcadia/contrib/python/responses/py2/responses/test_responses.py
)

SRCDIR(contrib/python/responses/py2/responses)

TEST_SRCS(
    test_matchers.py
    test_responses.py
)

NO_LINT()

END()
