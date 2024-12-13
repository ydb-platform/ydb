LIBRARY()

PEERDIR(
    yql/essentials/core/issue/protos
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_ansi
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()

RECURSE_FOR_TESTS(
    ut
)
