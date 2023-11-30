LIBRARY()

SRCS(
    control_plane_events.cpp
    data_plane.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/events
)

END()
