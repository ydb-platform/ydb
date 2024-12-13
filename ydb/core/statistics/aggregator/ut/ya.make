UNITTEST_FOR(ydb/core/statistics/aggregator)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
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
    ut_analyze_datashard.cpp
    ut_analyze_columnshard.cpp
    ut_traverse_datashard.cpp
    ut_traverse_columnshard.cpp
)

END()
