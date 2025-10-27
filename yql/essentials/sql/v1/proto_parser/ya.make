LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
)

SRCS(
    parse_tree.cpp
    proto_parser.cpp
)

END()

RECURSE(
    antlr4
    antlr4_ansi
)
