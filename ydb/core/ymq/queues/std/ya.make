LIBRARY()

SRCS(
    queries.cpp
    schema.cpp
)

PEERDIR(
    ydb/core/ymq/base
    ydb/core/ymq/actor/cfg
    ydb/core/ymq/actor/cloud_events
)

END()
