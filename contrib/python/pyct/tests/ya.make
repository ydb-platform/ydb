PY3TEST()

PEERDIR(
    contrib/python/pyct
)

NO_LINT()

SRCDIR(contrib/python/pyct/pyct/tests)

TEST_SRCS(
    __init__.py
#    test_cmd.py
    test_report.py
)

END()
