PY3TEST()

PEERDIR(
    contrib/python/semver
)

TEST_SRCS(
    test_semver.py
    test_typeerror-274.py
)

NO_LINT()

END()
