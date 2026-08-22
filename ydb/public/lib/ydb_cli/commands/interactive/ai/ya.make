LIBRARY()

SRCS(
    ai_model_handler.cpp
)

PEERDIR(
    ydb/library/yverify_stream
    ydb/public/lib/ydb_cli/commands/interactive/ai/models
    ydb/public/lib/ydb_cli/commands/interactive/ai/tools
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/common
)

END()

RECURSE(
    models
    tools
)
