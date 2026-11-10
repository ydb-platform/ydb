PY3TEST()
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

TEST_SRCS(
    test_workload_manager.py
)

FORK_TESTS()
FORK_TEST_FILES()
FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:48 cpu:4)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    REQUIREMENTS(ram:16 cpu:1)
    SIZE(MEDIUM)
ENDIF()

ENV(YDB_ENABLE_COLUMN_TABLES="true")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")
ENV(WAIT_CLUSTER_ALIVE_TIMEOUT="60")

PEERDIR(
    ydb/tests/functional/tpc/lib
    ydb/tests/workload_manager/common
)

DATA(
    arcadia/ydb/tests/functional/clickbench/data/hits.csv
)
END()
