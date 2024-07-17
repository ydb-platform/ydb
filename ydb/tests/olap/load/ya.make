PY3TEST()

    TAG(ya:manual)

    TIMEOUT(600)

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS (
        test_clickbench.py
        test_tpcds.py
        test_tpch.py
    )

    PEERDIR (
        contrib/python/allure-pytest
        contrib/python/allure-python-commons
        ydb/public/sdk/python/enable_v3_new_behavior
        ydb/tests/olap/lib
        library/python/testing/yatest_common
        ydb/public/sdk/python
    )
    IF(NOT OPENSOURCE)
        DATA (
            sbr://6581137886
        )
    ENDIF()

    IF(NOT NOT_INCLUDE_CLI)
        DEPENDS (
            ydb/apps/ydb
        )
    ENDIF()
END()
