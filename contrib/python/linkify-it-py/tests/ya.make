PY3TEST()

PEERDIR(
    contrib/python/linkify-it-py
)

NO_LINT()

TEST_SRCS(
    __init__.py
    test_apis.py
    test_linkify.py
    utils.py
)

DATA(
    arcadia/contrib/python/linkify-it-py/tests/fixtures
)

END()
