LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/ast

    yql/essentials/parser/proto_ast/antlr3
    yql/essentials/parser/proto_ast/antlr4
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_ansi
    yql/essentials/parser/proto_ast/gen/v1_proto_split
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    proto_parser.cpp
)

END()
