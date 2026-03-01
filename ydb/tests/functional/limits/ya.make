PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    test_schemeshard_limits.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

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
