UNITTEST_FOR(ydb/library/yql/udfs/statistics_internal)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/statistics/ut_common
    ydb/core/testlib/default
)

SRCS(
    ut_udafs.cpp
)

END()
