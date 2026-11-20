PY3TEST()

TEST_SRCS(
    test_topic_replication.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

SIZE(MEDIUM)
REQUIREMENTS(ram:8 cpu:1)

END()
