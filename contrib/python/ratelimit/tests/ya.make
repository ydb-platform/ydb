PY3TEST()

PEERDIR(
    contrib/python/ratelimit
)

PY_SRCS(
    NAMESPACE tests
    __init__.py
)

TEST_SRCS(
    unit/__init__.py
    unit/decorator_test.py
)

NO_LINT()

END()
