PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_example.py
    test_export_s3.py
    test_followers.py
    test_compatibility.py
    test_stress.py
    test_statistics.py
    test_rolling.py
    test_data_type.py
    test_vector_index.py
    test_batch_operations.py
    test_transfer.py
    test_node_broker_delta_protocol.py
    udf/test_datetime2.py
    udf/test_digest.py
    udf/test_digest_regression.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:16)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)


DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/datashard/lib
    ydb/tests/stress/simple_queue/workload
    ydb/tests/library/compatibility
)

END()
