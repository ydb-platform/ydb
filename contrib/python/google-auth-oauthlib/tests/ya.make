PY3TEST()

PEERDIR(
    contrib/python/google-auth-oauthlib
    contrib/python/mock
    contrib/python/pytest-localserver
    contrib/python/click
)

DATA(
    arcadia/contrib/python/google-auth-oauthlib/tests/unit/data
)

TEST_SRCS(
    unit/test_flow.py
    unit/test_helpers.py
    unit/test_interactive.py
    unit/test_tool.py
)

NO_LINT()

END()
