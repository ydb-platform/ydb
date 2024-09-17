LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    yql_highlight.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    ydb/library/yql/parser/lexer_common
    ydb/library/yql/sql/v1/lexer
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
