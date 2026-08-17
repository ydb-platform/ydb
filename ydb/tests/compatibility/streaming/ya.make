PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_scalar_topic_write.py
    test_streaming.py
    test_watermarks.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
REQUIREMENTS(ram:16)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/tests/tools/pq_read
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/fq/streaming_common
    ydb/tests/library
    ydb/tests/library/compatibility
    ydb/tests/library/test_meta
    ydb/tests/tools/datastreams_helpers
)

END()
