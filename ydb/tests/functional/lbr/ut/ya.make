PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_collector.py
    test_conversion.py
)

PEERDIR(
    ydb/tests/functional/lbr/lib
)

END()
