PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_icv2_load.py
)

PEERDIR(
    ydb/tests/stability/icv2_load
)

END()
