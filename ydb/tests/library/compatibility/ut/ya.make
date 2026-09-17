PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
    ydb/tests/oss/ydb_sdk_import
)

TEST_SRCS(
    test_bootstrap_node_down.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)
TIMEOUT(300)

END()
