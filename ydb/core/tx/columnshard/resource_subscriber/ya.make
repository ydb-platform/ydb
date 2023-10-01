LIBRARY()

SRCS(
    actor.cpp
    counters.cpp
    task.cpp
    events.cpp
)

PEERDIR(
    ydb/core/protos
    library/cpp/actors/core
    ydb/core/tablet_flat
)

END()
