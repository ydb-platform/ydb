PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_secrets_monitoring.py
)

TAG(ya:fat)
SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    ydb/tests/functional/secrets/lib
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

END()
