PY3TEST()

FORK_TEST_FILES()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    contrib/python/boto3
    contrib/python/pyarrow
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
    test_bindings_0.py
    test_bindings_1.py
    test_compressions.py
    test_early_finish.py
    test_empty.py
    test_explicit_partitioning_0.py
    test_explicit_partitioning_1.py
    test_format_setting.py
    test_formats.py
    test_inflight.py
    test_insert.py
    test_public_metrics.py
    test_push_down.py
    test_s3_0.py
    test_s3_1.py
    test_streaming_join.py
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

DATA(
    arcadia/ydb/tests/fq/s3
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR SANITIZER_TYPE == "address")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
