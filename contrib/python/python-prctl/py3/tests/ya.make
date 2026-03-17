PY3TEST()

PEERDIR(
    contrib/python/python-prctl
)

NO_LINT()

SRCDIR(contrib/python/python-prctl/py3)

TEST_SRCS(
    test_prctl.py
)

END()
