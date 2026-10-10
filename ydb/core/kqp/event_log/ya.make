LIBRARY()

SRCS(
    base_event_log_writer.cpp
    column_shard_log_writer.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/protos
    ydb/core/tx/columnshard
    ydb/core/wrappers
    ydb/library/actors/core
)


YQL_LAST_ABI_VERSION()

END()
