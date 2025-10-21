UNITTEST_FOR(ydb/core/statistics/agg_funcs/udfs)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/statistics/ut_common
    ydb/core/statistics/agg_funcs/udfs
    ydb/core/testlib/default
)

SRCS(
    ut_udafs.cpp
)

END()
