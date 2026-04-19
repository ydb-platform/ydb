PROGRAM(ydb)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/apps/ydb/commands
    ydb/public/lib/ydb_cli/commands
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/sdk/cpp/src/client/topic/codecs
)

END()
