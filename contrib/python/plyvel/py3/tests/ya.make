PY3TEST()

PEERDIR(
    contrib/python/plyvel
)

TEST_SRCS(
    __init__.py
    test_plyvel.py
)

NO_LINT()

END()
