PY3TEST()

    PY_SRCS (
        conftest.py
    )

    FORK_TESTS()

    TEST_SRCS(
        test_alter_compression.py
        test_alter_tiering.py
        test_insert.py
        test_read_update_write_load.py
        test_scheme_load.py
        test_simple.py
    )

    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    DEPENDS(
        )

    PEERDIR(
        contrib/python/Flask
        contrib/python/Flask-Cors
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/boto3
        contrib/python/moto
        contrib/python/requests
        library/python/testing/yatest_common
        library/recipes/common
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/library
        ydb/tests/olap/common
        ydb/tests/olap/lib
        ydb/tests/olap/scenario/helpers
    )

    SIZE(MEDIUM)

END()
