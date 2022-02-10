LIBRARY()

OWNER(
    g:yql
    g:yql_ydb_core
)

PEERDIR(
    ydb/library/yql/core/issue/protos 
    ydb/library/yql/parser/proto_ast 
    ydb/library/yql/parser/proto_ast/gen/v0 
)

SRCS(
    lexer.cpp
)

END()
