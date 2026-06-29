PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(


    test_example.py
)


DEPENDS(
    ydb/apps/ydb
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()


PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
)

END()
