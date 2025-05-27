LIBRARY()

PROVIDES(
    yql_pg_sql_translator
    yql_pg_runtime
)

PEERDIR(
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/minikql
    yql/essentials/sql/settings
)

SRCS(
    pg_sql_dummy.cpp
)

YQL_LAST_ABI_VERSION()

END()
