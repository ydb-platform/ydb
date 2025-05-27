UNITTEST_FOR(yql/essentials/sql/v0)

SRCS(
    sql_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
