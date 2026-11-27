LIBRARY()

PEERDIR(
    yql/essentials/public/issue/protos
    yql/essentials/sql/settings
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_split_code/lib
)

END()

RECURSE(
    antlr4
    antlr4_ansi
    antlr4_pure
    antlr4_pure_ansi
    check
    regex
)

RECURSE_FOR_TESTS(
    ut
)

