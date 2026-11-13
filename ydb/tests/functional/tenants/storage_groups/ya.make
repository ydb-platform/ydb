PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)

TEST_SRCS(
    ydb/tests/functional/tenants/conftest.py
    ydb/tests/functional/tenants/test_remove_storage_groups.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
    ydb/tests/stress/remove_storage_groups/workload
    ydb/tests/stress/tpcc/workload
    ydb/public/sdk/python
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ENDIF()

END()
