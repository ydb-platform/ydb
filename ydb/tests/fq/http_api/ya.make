PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    library/python/retry
    ydb/core/fq/libs/http_api_client
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
)

PY_SRCS(
    test_base.py
)

TEST_SRCS(
    test_http_api.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
