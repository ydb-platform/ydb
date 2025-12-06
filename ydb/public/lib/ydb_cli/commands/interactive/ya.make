LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    ydb/core/base
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/commands/interactive/complete
    ydb/public/lib/ydb_cli/commands/interactive/session
    yql/essentials/sql/v1/complete/text
)

END()

RECURSE(
    complete
    highlight
    session
)
