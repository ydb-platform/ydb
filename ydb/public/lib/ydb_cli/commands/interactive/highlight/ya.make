LIBRARY()

SRCS(
    yql_highlighter.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/highlight
    ydb/public/lib/ydb_cli/commands/interactive/highlight/color
)

END()

RECURSE(
    color
)

RECURSE_FOR_TESTS(
    ut
)
