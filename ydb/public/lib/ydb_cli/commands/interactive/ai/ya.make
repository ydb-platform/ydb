LIBRARY()

SRCS(
    ai_model_handler.cpp
)

PEERDIR(
    ydb/core/base
    ydb/public/lib/ydb_cli/commands/interactive/ai/models
    ydb/public/lib/ydb_cli/commands/interactive/common
)

END()

RECURSE(models)
