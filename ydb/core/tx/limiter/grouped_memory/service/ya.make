LIBRARY()

SRCS(
    actor.cpp
    manager.cpp
    counters.cpp
    group.cpp
    process.cpp
    allocation.cpp
    ids.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/counters/common
)

END()
