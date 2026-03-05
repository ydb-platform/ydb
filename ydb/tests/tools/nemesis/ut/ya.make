SUBSCRIBER(g:kikimr)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_disk.py
    test_tablet.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()


DEPENDS(
)

PEERDIR(
    ydb/tests/tools/nemesis/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
