PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    hive_matchers.py
    test_create_tablets.py
    test_kill_tablets.py
    test_drain.py
)


REQUIREMENTS(
    cpu:1
    ram:16
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
        cpu:1
    )
    SPLIT_FACTOR(20)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    SPLIT_FACTOR(20)
ENDIF()

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
