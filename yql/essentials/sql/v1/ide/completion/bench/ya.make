G_BENCHMARK()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/name/service/ranking
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/pure_ast
    yql/essentials/utils/string
)

END()
