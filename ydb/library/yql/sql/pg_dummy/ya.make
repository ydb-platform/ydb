LIBRARY()

PROVIDES(
    yql_pg_sql_translator
    yql_pg_runtime
)

PEERDIR(
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/minikql
)

SRCS(
    pg_sql_dummy.cpp
    pg_sql_dummy.c
)

YQL_LAST_ABI_VERSION()

END()
