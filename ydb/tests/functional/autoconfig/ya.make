PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    test_actorsystem.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:1)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
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
