PY3TEST()

SIZE(MEDIUM)
TIMEOUT(120)

TEST_SRCS(
    test_client.py
    test_connection.py
)

PEERDIR(
    ydb/apps/dstool/lib
    ydb/apps/dstool/tui
)

END()
