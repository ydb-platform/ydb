PY2TEST()

PEERDIR(
    contrib/python/python-prctl
)

NO_LINT()

SRCDIR(contrib/python/python-prctl/py2)

TEST_SRCS(
    test_prctl.py
)

END()
