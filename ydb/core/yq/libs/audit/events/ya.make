LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/yq/libs/checkpointing_common
    ydb/core/yq/libs/control_plane_storage/events
    ydb/public/api/protos
    ydb/library/yql/public/issue
)

END()
