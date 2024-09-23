LIBRARY()

PEERDIR(
    library/cpp/deprecated/split
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/sql/settings
    ydb/library/yql/sql/v0
    ydb/library/yql/sql/v0/lexer
    ydb/library/yql/sql/v1
    ydb/library/yql/sql/v1/format
    ydb/library/yql/sql/v1/lexer
    ydb/library/yql/sql/v1/proto_parser
    ydb/library/yql/utils
)

SRCS(
    cluster_mapping.cpp
    sql.cpp
)

END()

RECURSE(
    pg
    pg_dummy
    settings
    v0
    v1
)
