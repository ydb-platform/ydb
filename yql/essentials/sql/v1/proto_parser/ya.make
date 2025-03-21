LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/collect_issues
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

