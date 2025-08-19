UNITTEST_FOR(ydb/library/yql/sql/pg)

TAG(ya:manual)

SRCS(
    pg_sql_ut.cpp
    pg_sql_autoparam_ut.cpp
    optimizer_ut.cpp
    optimizer_impl_ut.cpp
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/libs/fmt

    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
)

SIZE(MEDIUM)

END()
