LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/antlr4
    yql/essentials/parser/proto_ast/gen/v1_antlr4
)

SRCS(
    parser_cache.cpp
    proto_parser.cpp
)

END()
