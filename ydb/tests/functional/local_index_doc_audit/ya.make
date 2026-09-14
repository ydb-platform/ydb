PY3TEST()

SIZE(MEDIUM)
TIMEOUT(300)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_local_index_doc_audit.py
)

PEERDIR(
    ydb/tests/library
)

END()
