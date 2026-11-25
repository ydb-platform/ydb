LIBRARY()

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/public/issue
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_antlr4
)

SRCS(
    lexer.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_split_code/lib
)

END()
