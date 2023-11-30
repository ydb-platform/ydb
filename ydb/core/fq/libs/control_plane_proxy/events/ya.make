LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/core/fq/libs/control_plane_storage/events
    ydb/core/fq/libs/quota_manager/events
)

END()
