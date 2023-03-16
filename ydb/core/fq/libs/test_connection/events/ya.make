LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/events
    ydb/library/yql/public/issue/protos
    ydb/public/api/protos
)

END()
