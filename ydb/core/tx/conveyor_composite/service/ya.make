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
    workload.cpp
)

PEERDIR(
    ydb/core/cms/console
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/tx/conveyor_composite/tracing
    ydb/core/tx/conveyor_composite/usage
    ydb/core/util
)

END()
