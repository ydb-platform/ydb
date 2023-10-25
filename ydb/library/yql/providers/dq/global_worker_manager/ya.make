LIBRARY()

PEERDIR(
    ydb/library/yql/utils/failure_injector
    ydb/library/yql/providers/common/config
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/config
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/actors/yt
    ydb/library/yql/providers/dq/scheduler
    ydb/library/yql/providers/dq/service
)

YQL_LAST_ABI_VERSION()

SET(
    SOURCE
    benchmark.cpp
    global_worker_manager.cpp
    service_node_pinger.cpp
    workers_storage.cpp
    worker_filter.cpp
)

IF (NOT OS_WINDOWS)
    SET(
        SOURCE
        ${SOURCE}
        service_node_resolver.cpp
        coordination_helper.cpp
    )
ELSE()
    SET(
        SOURCE
        ${SOURCE}
        coordination_helper_win.cpp
    )
ENDIF()

SRCS(
    ${SOURCE}
)

END()

RECURSE_FOR_TESTS(
    ut
)
