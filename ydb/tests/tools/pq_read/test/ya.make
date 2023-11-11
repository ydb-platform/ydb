PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    ydb/tests/tools/datastreams_helpers
)

DEPENDS(ydb/tests/tools/pq_read)

TEST_SRCS(
    test_commit.py
    test_timeout.py
)

END()
