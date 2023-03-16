LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/events
    ydb/core/fq/libs/checkpoint_storage/proto
    ydb/library/yql/public/issue
)

END()
