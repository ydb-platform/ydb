PY3TEST()

TEST_SRCS(
    test_tpch_restarts.py
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

REQUIREMENTS(ram:32 cpu:8)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="3221225472")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

PEERDIR(
    ydb/tests/library
)

END()
