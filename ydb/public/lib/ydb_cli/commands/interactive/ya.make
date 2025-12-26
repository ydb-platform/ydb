LIBRARY()

SRCS(
    interactive_cli.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/commands/interactive/complete
    ydb/public/lib/ydb_cli/commands/interactive/session
)

IF (NOT OS_WINDOWS OR USE_SSE4)
    CFLAGS(-DYDB_CLI_AI_ENABLED=1)
ENDIF()

END()

RECURSE(
    ai
    complete
    highlight
    session
)
