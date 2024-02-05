PY3TEST()

FORK_SUBTESTS()
SPLIT_FACTOR(50)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    ydb/public/api/protos
    ydb/public/api/grpc
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
)

DEPENDS(ydb/tests/tools/pq_read)

PY_SRCS(
    conftest.py
    test_base.py
)

TEST_SRCS(
    test_recovery_match_recognize.py
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(ram:16)

END()
