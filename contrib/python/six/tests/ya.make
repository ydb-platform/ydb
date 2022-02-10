PY23_TEST()

OWNER(g:python-contrib)

PEERDIR(
    contrib/python/six
)

TEST_SRCS(
    test_six.py
)

NO_LINT()

END()
