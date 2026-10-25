PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_actorsystem.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
    IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
        REQUIREMENTS(cpu:2)
    ELSE()
        REQUIREMENTS(cpu:4)
    ENDIF()
ENDIF()

SPLIT_FACTOR(20)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
