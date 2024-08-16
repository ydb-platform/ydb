SUBSCRIBER(g:kikimr)

PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_disk.py
    test_tablet.py
)

TIMEOUT(600)
SIZE(MEDIUM)


DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/tools/nemesis/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
