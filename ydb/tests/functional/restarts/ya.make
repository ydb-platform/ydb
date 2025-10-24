PY3TEST()

TEST_SRCS(
    test_restarts.py
)

SPLIT_FACTOR(10)
REQUIREMENTS(ram:16 cpu:4)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
