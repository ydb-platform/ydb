G_BENCHMARK()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/name/service/ranking
    yql/essentials/sql/v1/complete/name/service/static
)

END()
