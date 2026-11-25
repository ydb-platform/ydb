LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/parser/common/antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
)

SRCS(
    lexer.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_split_code/lib
)

END()
