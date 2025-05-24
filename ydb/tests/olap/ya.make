PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        test_log_scenario.py
        zip_bomb.py
    )
    FORK_SUBTESTS()

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        TAG(ya:fat)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    DEPENDS(
        ydb/apps/ydb
        ydb/apps/ydbd
    )

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
        ydb/tests/olap/lib
    )
END()

RECURSE(
    column_family
    common
    docs
    high_load
    lib
    load
    oom
    s3_import
    scenario
    ttl_tiering
    data_quotas
)
