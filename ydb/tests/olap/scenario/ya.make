PY3TEST()

    TAG(ya:manual)

    TIMEOUT(600)

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS(
        test_simple.py
        test_scheme_load.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/pandas
        contrib/python/requests
        ydb/public/sdk/python
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/olap/scenario/helpers
        library/python/testing/yatest_common
    )

END()
