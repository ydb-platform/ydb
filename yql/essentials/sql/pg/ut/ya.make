UNITTEST_FOR(yql/essentials/sql/pg)

SRCS(
    pg_sql_ut.cpp
    pg_sql_autoparam_ut.cpp
    optimizer_ut.cpp
    optimizer_impl_ut.cpp
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/libs/fmt

    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SIZE(MEDIUM)

END()
