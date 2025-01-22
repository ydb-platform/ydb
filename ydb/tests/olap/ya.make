PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

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
    )
END()

RECURSE(
    lib
    scenario
    docs
    load
    ttl_tiering
)
