IF (OS_LINUX AND NOT SANITIZER_TYPE)

PY3TEST()

TEST_SRCS(
    test_break.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

PEERDIR(
    ydb/tests/library
)

DEPENDS(
)


END()

ENDIF()
