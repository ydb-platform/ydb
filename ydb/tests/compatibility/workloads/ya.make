PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_simple_reader.py
    test_table_schema_compatibility.py
    test_workload_manager.py
    test_statistics.py
    test_compact.py
    test_in_memory.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:8)
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
