LIBRARY()

SRCS(
    yql_highlighter.cpp
)

PEERDIR(
    contrib/libs/ftxui
    contrib/restricted/patched/replxx
    ydb/public/lib/ydb_cli/commands/interactive/highlight/color
    ydb/public/lib/ydb_cli/common
    yql/essentials/sql/v1/highlight
)

END()

RECURSE(
    color
)

RECURSE_FOR_TESTS(
    ut
)
