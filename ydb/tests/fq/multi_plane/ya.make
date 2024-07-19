PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
)

DEPENDS(
    ydb/tests/tools/pq_read
)

TEST_SRCS(
    test_cp_ic.py
    test_dispatch.py
    test_retry.py
)

TIMEOUT(600)

SIZE(MEDIUM)

END()
