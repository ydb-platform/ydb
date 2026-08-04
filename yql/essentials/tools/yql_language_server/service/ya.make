LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/support
    yql/essentials/public/sql_format
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
)

SRCS(
    completion.cpp
    formatting.cpp
    layer.cpp
)

END()
