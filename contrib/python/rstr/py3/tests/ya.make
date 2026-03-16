PY3TEST()

PEERDIR(
    contrib/python/rstr
)

TEST_SRCS(
    test_package_level_access.py
    test_rstr.py
    test_xeger.py
)

NO_LINT()

END()
