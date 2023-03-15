LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/yq/libs/control_plane_storage/events
    ydb/core/yq/libs/events
    ydb/public/api/protos
    ydb/library/yql/public/issue/protos
)

END()
