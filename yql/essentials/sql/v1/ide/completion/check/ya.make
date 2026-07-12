LIBRARY()

SRCS(
    check_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion
    yql/essentials/sql/v1/ide/completion/analysis/yql
    yql/essentials/sql/v1/ide/completion/name/cluster/static
    yql/essentials/sql/v1/ide/completion/name/object/simple/static
    yql/essentials/sql/v1/ide/completion/name/service/cluster
    yql/essentials/sql/v1/ide/completion/name/service/schema
    yql/essentials/sql/v1/ide/completion/name/service/static
    yql/essentials/sql/v1/ide/completion/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
