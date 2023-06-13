UNITTEST_FOR(ydb/library/yql/sql/pg)

SRCS(
    pg_sql_ut.cpp
)

PEERDIR(
    contrib/libs/fmt

    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg
)

SIZE(MEDIUM)

END()
