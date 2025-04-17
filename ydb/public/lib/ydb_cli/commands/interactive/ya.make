LIBRARY()

SRCS(
    interactive_cli.cpp
    line_reader.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/commands/interactive/complete
)

END()

RECURSE(
    complete
    highlight
)
