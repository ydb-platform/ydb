PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_example.py
    test_followers.py
    test_fulltext_index.py
    test_in_memory.py
    test_compatibility.py
    test_stress.py
    test_statistics.py
    test_system_views.py
    test_simple_reader.py
    test_rolling.py
    test_data_type.py
    test_ctas.py
    test_vector_index.py
    test_batch_operations.py
    test_topic.py
    test_kafka_topic.py
    test_transfer.py
    test_node_broker_delta_protocol.py
    test_table_schema_compatibility.py
    test_user_management.py
    test_workload_manager.py
    test_default_columns.py
    test_infer_pdisk_expected_slot_count.py
    test_show_create_table.py
    udf/test_datetime2.py
    udf/test_digest.py
    udf/test_digest_regression.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:16)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/datashard/lib
    ydb/tests/stress/simple_queue/workload
    ydb/tests/library/compatibility
)

END()

RECURSE(
    federated_queries
    s3_backups
    olap
    streaming
    result_set_format
    distconf
)
