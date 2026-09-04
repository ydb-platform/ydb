PY3_LIBRARY()

    PY_SRCS (
        workload_manager.py

    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/matplotlib
        contrib/python/pytest-timeout
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        ydb/tests/olap/load/lib
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
