PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(50)
    FORK_SUBTESTS()
    REQUIREMENTS(ram:32 cpu:4)
ELSE()
    # Reuse module-scoped clusters and hostels within each configuration.
    SPLIT_FACTOR(8)
    FORK_SUBTESTS(MODULO)
    REQUIREMENTS(cpu:1)
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
