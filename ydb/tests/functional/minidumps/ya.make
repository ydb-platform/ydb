IF (OS_LINUX AND NOT SANITIZER_TYPE)

PY3TEST()

TEST_SRCS(
    test_break.py
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

PEERDIR(
    ydb/tests/library
)

DEPENDS(
)


END()

ENDIF()
