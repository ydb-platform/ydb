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
    test_in_memory.py
    test_compatibility.py
    test_statistics.py
    test_system_views.py
    test_simple_reader.py
    test_data_type.py
    test_ctas.py
    test_batch_operations.py
    test_compact.py
    test_node_broker_delta_protocol.py
    test_system_tablet_backup.py
    test_table_schema_compatibility.py
    test_user_management.py
    test_workload_manager.py
    test_default_columns.py
    test_infer_pdisk_expected_slot_count.py
    test_show_create_table.py
    test_snapshot_isolation.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:4)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/datashard/lib
    ydb/tests/library/compatibility
)

END()
