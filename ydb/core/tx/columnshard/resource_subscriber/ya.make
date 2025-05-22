LIBRARY()

SRCS(
    actor.cpp
    counters.cpp
    task.cpp
    events.cpp
    container.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()
