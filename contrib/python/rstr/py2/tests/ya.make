PY2TEST()

PEERDIR(
    contrib/python/rstr
)

SRCDIR(contrib/python/rstr/py2/rstr/tests)

TEST_SRCS(
    test_package_level_access.py
    test_rstr.py
    test_xeger.py
)

NO_LINT()

END()
