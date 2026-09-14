PY3TEST()

SIZE(MEDIUM)
TIMEOUT(300)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_global_index_doc_audit.py
)

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
)

END()
