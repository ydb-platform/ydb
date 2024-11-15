UNITTEST_FOR(ydb/core/statistics/service)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/statistics/ut_common
)

SRCS(
    ut_basic_statistics.cpp
    ut_column_statistics.cpp
    ut_http_request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut_aggregation
)
