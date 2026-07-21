PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=false)

TEST_SRCS(
    test_pq_head_rss.py
)

TAG(ya:not_autocheck)
SIZE(LARGE)
REQUIREMENTS(ram:16 cpu:2)
FORK_SUBTESTS()

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

END()
