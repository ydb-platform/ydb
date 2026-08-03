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
    ydb/core/tx/columnshard/engines/protos  # stopgap: columnshard_private_events.h transitively requires engines/protos; direct columnshard dep would create a cycle
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()
