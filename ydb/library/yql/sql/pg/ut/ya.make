UNITTEST_FOR(ydb/library/yql/sql/pg)

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
REQUIREMENTS(cpu:1)

END()
