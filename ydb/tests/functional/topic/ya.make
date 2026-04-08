PY3TEST()

FORK_SUBTESTS()
FORK_TEST_FILES()
SPLIT_FACTOR(100)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    helpers.py
    test_topic_audit.py
)

DATA(
    arcadia/ydb/tests/functional/topic/canondata
)

PEERDIR(
    contrib/python/protobuf
    ydb/core/persqueue/public/cloud_events/proto
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/canonical
)

END()
