PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

SPLIT_FACTOR(50)
FORK_TEST_FILES()
FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
DEPENDS(
)

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

END()
