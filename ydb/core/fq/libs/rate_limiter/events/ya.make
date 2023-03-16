LIBRARY()

SRCS(
    control_plane_events.cpp
    data_plane.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/fq/libs/events
)

END()
