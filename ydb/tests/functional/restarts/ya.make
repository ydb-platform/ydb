PY3TEST()

TEST_SRCS(
    test_restarts.py
)

FORK_SUBTESTS()
SPLIT_FACTOR(8)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
    IF (SANITIZER_TYPE == "thread")
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    ENDIF()
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

END()
