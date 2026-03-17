PY3TEST()

PEERDIR(
    contrib/python/pytest-responsemock
)

TEST_SRCS(
    test_module.py
)

NO_LINT()

END()
