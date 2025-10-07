LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_proto_split
)

SRCS(
    parse_tree.cpp
    proto_parser.cpp
)

END()

RECURSE(
    antlr3
    antlr3_ansi
    antlr4
    antlr4_ansi
)
