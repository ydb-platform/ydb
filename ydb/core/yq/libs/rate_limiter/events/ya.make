LIBRARY()

SRCS(
    control_plane_events.cpp
    data_plane.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/yq/libs/events
)

END()
