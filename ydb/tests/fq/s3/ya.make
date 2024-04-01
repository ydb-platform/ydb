PY3TEST()

FORK_TEST_FILES()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/boto3
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
)

DEPENDS(
    contrib/python/moto/bin
    ydb/tests/tools/pq_read
)

TEST_SRCS(
    test_bindings.py
    test_compressions.py
    test_early_finish.py
    test_empty.py
    test_explicit_partitioning.py
    test_format_setting.py
    test_formats.py
    test_inflight.py
    test_insert.py
    test_public_metrics.py
    test_push_down.py
    test_s3.py
    test_size_limit.py
    test_statistics.py
    test_test_connection.py
    test_ydb_over_fq.py
    test_yq_v2.py
)

PY_SRCS(
    conftest.py
    s3_helpers.py
)

DATA(arcadia/ydb/tests/fq/s3)

IF (SANITIZER_TYPE == "thread" OR SANITIZER_TYPE == "address")
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

REQUIREMENTS(ram:16)

END()
