LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    yql_highlight.cpp
    yql_position.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/parser/lexer_common
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/settings
    yql/essentials/utils
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
