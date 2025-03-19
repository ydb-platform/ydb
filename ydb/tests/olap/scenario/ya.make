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
        contrib/python/boto3
        contrib/python/requests
        contrib/python/moto
        contrib/python/Flask
        contrib/python/Flask-Cors
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/library
        ydb/tests/olap/scenario/helpers
        ydb/tests/olap/common
        library/python/testing/yatest_common
        library/recipes/common
    )

    SIZE(MEDIUM)

END()
