PY3TEST()

PEERDIR(
    contrib/python/falcon
    contrib/python/pytest
    contrib/python/pytest-falcon
    contrib/python/falcon-multipart
)

DATA(
    arcadia/contrib/python/falcon-multipart/tests
)

TEST_SRCS(
    test_middleware.py
)

END()
