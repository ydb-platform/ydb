PY3TEST()

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS(
        test_simple.py
        test_scheme_load.py
        test_alter_tiering.py
        test_insert.py
        test_alter_compression.py
    )

    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
    DEPENDS(
        ydb/apps/ydbd
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/pandas
        contrib/python/requests
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/library
        ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
    )

    SIZE(MEDIUM)

END()
