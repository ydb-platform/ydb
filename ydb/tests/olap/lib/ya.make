PY3_LIBRARY()

    SUBSCRIBER(iddqd)

    PY_SRCS (
        allure_utils.py
        results_processor.py
        ydb_cluster.py
        utils.py
        ydb_cli.py
    )

    PEERDIR(
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        contrib/python/requests
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )

END()
