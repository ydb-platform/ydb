PY3TEST()

PEERDIR(
    contrib/python/pytest-falcon
)

DATA(
    sbr://408210239
)

TEST_SRCS(
    test_fixtures.py
    test_multipart.py
)

NO_LINT()

END()
