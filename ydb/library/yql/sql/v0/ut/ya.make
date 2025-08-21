UNITTEST_FOR(ydb/library/yql/sql/v0)

TAG(ya:manual)

SRCS(
    sql_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
