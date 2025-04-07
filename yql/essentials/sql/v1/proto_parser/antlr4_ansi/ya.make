LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/antlr4
    yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    proto_parser.cpp
)

END()
