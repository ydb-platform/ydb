UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/core/testlib/default
    ydb/core/formats
    ydb/core/tx
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_olap.cpp
    column_name_validation_ut.cpp
)

END()
