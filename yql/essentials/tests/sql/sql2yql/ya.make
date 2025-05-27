IF (NOT OPENSOURCE)

PY3TEST()
    TEST_SRCS(
        test_sql2yql.py
        test_sql_negative.py
        test_sql_format.py
    )

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:12)
ENDIF()

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

    FORK_TESTS()
    FORK_SUBTESTS()
    SPLIT_FACTOR(5)
    DEPENDS(
        yql/essentials/tools/sql2yql
        yql/essentials/tools/sql_formatter
        contrib/libs/protobuf/python
    )
    DATA(
        arcadia/yql/essentials/tests/sql/suites
        arcadia/yql/essentials/mount
        arcadia/yql/essentials/cfg/tests
    )
    PEERDIR(
        yql/essentials/tests/common/test_framework
        library/python/testing/swag/lib
    )


NO_CHECK_IMPORTS()

END()

ENDIF()

