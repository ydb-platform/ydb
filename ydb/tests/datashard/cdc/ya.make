PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_cdc.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    contrib/python/requests
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/library/clients
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
