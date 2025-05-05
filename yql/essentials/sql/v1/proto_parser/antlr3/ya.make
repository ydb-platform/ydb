LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/antlr3
    yql/essentials/parser/proto_ast/gen/v1
)

SRCS(
    proto_parser.cpp
)

END()
