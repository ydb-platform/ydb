PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_type_support_consistency.py
)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    ydb/tests/library
)

END()
