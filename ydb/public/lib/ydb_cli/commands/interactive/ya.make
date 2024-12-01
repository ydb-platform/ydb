LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    yql_highlight.cpp
    yql_position.cpp
    yql_suggest.cpp
    yql_syntax.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    yql/essentials/parser/lexer_common
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/format
    yql/essentials/sql/settings
    yql/essentials/utils
    contrib/restricted/patched/replxx
    contrib/libs/antlr4_cpp_runtime
    contrib/libs/antlr4-c3
)

END()

RECURSE_FOR_TESTS(
    ut
)
