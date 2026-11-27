UNITTEST_FOR(ydb/core/tx/columnshard/mediator_time)

SIZE(SMALL)

SRCS(
    ut_catch_up.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
