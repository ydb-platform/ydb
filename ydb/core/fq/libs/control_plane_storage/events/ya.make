LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/events
    ydb/core/fq/libs/quota_manager/events
)

END()
