PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        test_log_scenario.py
    )
    FORK_SUBTESTS()
    SPLIT_FACTOR(100)

    SIZE(LARGE)
    TAG(ya:fat)

    DEPENDS(
        ydb/apps/ydb
        )

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
        ydb/tests/olap/lib
    )
END()
