PY3TEST()

    TAG(ya:manual)

    TIMEOUT(600)

    PY_SRCS (
        conftest.py
    )

    TEST_SRCS (
        test_clickbench.py
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
    IF(OPENSOURCE != "yes")
        DATA (
            sbr://6332216525
        )
    ENDIF()

    DEPENDS (
        ydb/apps/ydb
    )

END()
