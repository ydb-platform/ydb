LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    string_util.cpp
    yql_complete.cpp
    yql_highlight.cpp
    yql_name_source.cpp
    yql_position.cpp
    yql_syntax.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    contrib/libs/antlr4_cpp_runtime
    contrib/libs/antlr4-c3
    yql/essentials/parser/lexer_common
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/format
    yql/essentials/sql/settings
    yql/essentials/utils
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
