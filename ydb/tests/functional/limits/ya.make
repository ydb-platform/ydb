PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_schemeshard_limits.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

SIZE(MEDIUM)
IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(cpu:2)
ELSE()
    REQUIREMENTS(cpu:4)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

FORK_TEST_FILES()
FORK_SUBTESTS()

END()
