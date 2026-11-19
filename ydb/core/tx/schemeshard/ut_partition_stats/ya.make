UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(SMALL)

SRCS(
    ut_top_cpu_usage.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/pg
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
