LIBRARY()

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/public/issue
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_ansi
)

SRCS(
    lexer.cpp
)

END()
