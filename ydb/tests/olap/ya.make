PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        test_quota_exhaustion.py
    )

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
        ydb/tests/olap/lib
        ydb/tests/olap/scenario/helpers
        ydb/tests/olap/common
        ydb/tests/olap/helpers
        library/recipes/common
    )
END()

RECURSE(
    lib
    scenario
    docs
    load
    ttl_tiering
    column_family
    common
    helpers
)
