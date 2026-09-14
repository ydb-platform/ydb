PY3TEST()

SIZE(MEDIUM)
TIMEOUT(600)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_fulltext_hybrid_doc_audit.py
)

PEERDIR(
    ydb/tests/library
)

END()
