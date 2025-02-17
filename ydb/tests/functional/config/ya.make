PY3TEST()

TEST_SRCS(
    test_config_with_metadata.py
)

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()


ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
