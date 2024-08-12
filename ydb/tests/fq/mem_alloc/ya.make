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
    test_alloc_default.py
    test_dc_local.py
    test_result_limits.py
    test_scheduling.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:9)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
