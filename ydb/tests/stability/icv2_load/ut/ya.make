PY3TEST()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

TEST_SRCS(
    test_icv2_load.py
)

PEERDIR(
    ydb/tests/stability/icv2_load
)

END()
