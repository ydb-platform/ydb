LIBRARY()

PEERDIR(
    yql/essentials/tools/yql_language_server/lsp/support
    yql/essentials/public/fastcheck
    yql/essentials/public/sql_format
    yql/essentials/core/issue
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
)

SRCS(
    completion.cpp
    diagnostic.cpp
    formatting.cpp
    layer.cpp
    radix.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
