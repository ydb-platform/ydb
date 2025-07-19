LIBRARY()

SRCS(
    check_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/complete/analysis/yql
    yql/essentials/sql/v1/complete/name/cluster/static
    yql/essentials/sql/v1/complete/name/object/simple/static
    yql/essentials/sql/v1/complete/name/service/cluster
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
