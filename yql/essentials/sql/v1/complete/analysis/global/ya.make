LIBRARY()

SRCS(
    evaluate.cpp
    global.cpp
    use.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
)

END()
