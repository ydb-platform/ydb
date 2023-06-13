PY3TEST()

TEST_SRCS(
    test_yatest_common.py
)

SIZE(SMALL)

PEERDIR(
    library/python/testing/yatest_common_standalone
)

END()
