PY3TEST()

TEST_SRCS(
    test_tpcds.py
)

# https://github.com/ydb-platform/ydb/issues/15726
IF (SANITIZER_TYPE != "memory" AND SANITIZER_TYPE != "thread")
    TEST_SRCS(
        test_tpch_spilling.py
    )
ENDIF()

SIZE(LARGE)

REQUIREMENTS(ram:16 cpu:2)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")
ENV(WAIT_CLUSTER_ALIVE_TIMEOUT="60")

PEERDIR(
    ydb/tests/functional/tpc/lib
)

DEPENDS(
    ydb/apps/ydb
)

# ASAN whitelist run 39512661565: test_tpcds.py is one chunk and spends the
# whole SIZE(LARGE) hour (~33 min on query 1, then ~50 more S1 queries).
# The other ~246 tests are not launched. 8-way split keeps a chunk that
# contains that query inside 3600s. With FORK_TEST_FILES this is 8 chunks
# per file.
FORK_TEST_FILES()
SPLIT_FACTOR(8)

END()
