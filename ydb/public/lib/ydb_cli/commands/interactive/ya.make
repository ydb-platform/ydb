LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    yql_highlight.cpp
    yql_position.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    ydb/library/yql/parser/lexer_common
    ydb/library/yql/sql/v1/lexer
    ydb/library/yql/sql/settings
    ydb/library/yql/utils
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
