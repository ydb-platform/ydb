LIBRARY()

SRCS(
    yql_highlighter.cpp
    yql_position.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/parser/lexer_common
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/sql/v1/complete
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/settings
    yql/essentials/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
