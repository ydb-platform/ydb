PY3TEST()

    TAG(ya:manual)

    SIZE(LARGE)

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
        ydb/tests/olap/scenario/helpers
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
