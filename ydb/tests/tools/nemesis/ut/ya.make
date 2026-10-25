SUBSCRIBER(g:kikimr)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_disk.py
    test_tablet.py
)

SIZE(MEDIUM)
IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(cpu:3)
ELSE()
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
