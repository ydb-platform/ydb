LIBRARY()

OWNER(
    g:yql
    g:yql_ydb_core
)

PEERDIR(
    library/cpp/deprecated/split
    ydb/library/yql/sql/pg
    ydb/library/yql/sql/settings 
    ydb/library/yql/sql/v0 
    ydb/library/yql/sql/v0/lexer 
    ydb/library/yql/sql/v1 
    ydb/library/yql/sql/v1/lexer 
    ydb/library/yql/utils 
)

SRCS(
    cluster_mapping.cpp
    sql.cpp
)

END()
