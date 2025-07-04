LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/checkpoint_storage/proto
    yql/essentials/public/issue
)

END()
