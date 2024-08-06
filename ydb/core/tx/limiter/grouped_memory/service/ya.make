LIBRARY()

SRCS(
    actor.cpp
    manager.cpp
    counters.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/counters/common
)

END()
