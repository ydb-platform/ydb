UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(SMALL)

SRCS(
    ut_top_cpu_usage.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
