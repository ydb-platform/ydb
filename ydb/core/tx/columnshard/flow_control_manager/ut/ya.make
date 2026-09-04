UNITTEST_FOR(ydb/core/tx/columnshard/flow_control_manager)

SIZE(SMALL)

SRCS(
    ut_rate_control.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
