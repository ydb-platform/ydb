LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/ast

    yql/essentials/parser/proto_ast/collect_issues

    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

SRCS(
    proto_parser.cpp
)

END()

RECURSE(
    antlr3
    antlr3_ansi
    antlr4
    antlr4_ansi
)

