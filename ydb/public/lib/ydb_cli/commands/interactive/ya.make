LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
    yql_highlight.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    ydb/public/lib/ydb_cli/commands/interactive/antlr
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
