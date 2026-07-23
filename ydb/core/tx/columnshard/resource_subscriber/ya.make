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
    ydb/core/tx/columnshard/private_events
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()
