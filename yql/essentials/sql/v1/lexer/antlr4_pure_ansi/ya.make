LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/parser/common/antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
)

SRCS(
    lexer.cpp
)

END()
