LIBRARY()

SRCS(
    interactive_cli.cpp
)

PEERDIR(
    ydb/core/base
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/commands/interactive/complete
    ydb/public/lib/ydb_cli/commands/interactive/session
)

END()

RECURSE(
    ai
    complete
    highlight
    session
)
