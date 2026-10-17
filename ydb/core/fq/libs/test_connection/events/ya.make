LIBRARY()

# gn: move into parent

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/events
    yql/essentials/public/issue/protos
    ydb/public/api/protos
)

END()
