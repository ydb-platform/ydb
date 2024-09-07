PY3TEST()

PY_SRCS (
    utils.py
)
TEST_SRCS(
    test_cms_erasure.py
    test_cms_restart.py
    test_cms_state_storage.py
)

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
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
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
