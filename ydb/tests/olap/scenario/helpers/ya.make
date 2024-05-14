PY3_LIBRARY()

    SUBSCRIBER(iddqd)

    PY_SRCS (
        __init__.py
        scenario_tests_helper.py
        data_generators.py
        table_helper.py
        drop_helper.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        ydb/tests/olap/lib
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
