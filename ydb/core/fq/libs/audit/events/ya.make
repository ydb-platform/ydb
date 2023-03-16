LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/control_plane_storage/events
    ydb/library/yql/public/issue
    ydb/public/api/protos
)

END()
