LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/yq/libs/checkpointing_common
    ydb/core/yq/libs/events
    ydb/core/yq/libs/checkpoint_storage/proto
    ydb/library/yql/public/issue
)

END()
