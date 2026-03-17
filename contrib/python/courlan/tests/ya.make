PY3TEST()

PEERDIR(
    contrib/python/courlan
)

NO_LINT()

TEST_SRCS(
    __init__.py
    unit_tests.py
    urlstore_tests.py
)

DATA(
    arcadia/contrib/python/courlan/tests/data
)

END()
