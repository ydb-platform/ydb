LIBRARY()

PEERDIR(
    ydb/library/yql/core/issue/protos
    ydb/library/yql/parser/proto_ast
    ydb/library/yql/parser/proto_ast/gen/v1
    ydb/library/yql/parser/proto_ast/gen/v1_ansi
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
    ydb/library/yql/parser/proto_ast/gen/v1_antlr4
    ydb/library/yql/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()
