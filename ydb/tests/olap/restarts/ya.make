PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

    DEPENDS(
        ydb/apps/ydb
        ydb/apps/ydbd
    )

    TEST_SRCS(
        test_nodes_restart.py
    )

    SIZE(MEDIUM)

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        contrib/python/boto3
        library/recipes/common
        ydb/tests/olap/common
        ydb/tests/olap/helpers
    )

END()
