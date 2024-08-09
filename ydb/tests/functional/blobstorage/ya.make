PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    test_pdisk_format_info.py
    test_replication.py
    test_self_heal.py
    test_tablet_channel_migration.py
)

IF (SANITIZER_TYPE == "thread")
    REQUIREMENTS(
        ram:16
    )
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    REQUIREMENTS(
        ram:32
    )
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SPLIT_FACTOR(20)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
