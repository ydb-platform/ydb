LIBRARY()

SRCS(
    worker.cpp
    service.cpp
    process.cpp
    common.cpp
    manager.cpp
    workers_pool.cpp
    category.cpp
    scope.cpp
    counters.cpp
    events.cpp
)

PEERDIR(
    ydb/core/tx/conveyor_composite/usage
    ydb/core/protos
)

END()
