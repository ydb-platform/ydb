UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(
    ut_table_metrics_settings.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/pg
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
