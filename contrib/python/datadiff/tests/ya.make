PY3TEST()

PEERDIR(
    contrib/deprecated/python/nose
    contrib/python/datadiff
)

NO_LINT()

SRCDIR(contrib/python/datadiff)

TEST_SRCS(
    datadiff/tests/test_datadiff.py
    datadiff/tests/test_datadiff_tools.py
)

END()
