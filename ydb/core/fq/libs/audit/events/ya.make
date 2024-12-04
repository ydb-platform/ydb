LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/control_plane_storage/events
    yql/essentials/public/issue
    ydb/public/api/protos
)

END()
