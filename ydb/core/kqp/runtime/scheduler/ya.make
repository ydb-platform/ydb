YQL_LIBRARY()

SRCS(
    kqp_compute_scheduler.cpp
    kqp_schedulable_base.cpp
    kqp_schedulable_memory.cpp
    kqp_schedulable_read.cpp
    kqp_schedulable_task.cpp
    kqp_schedulable_work_factory.cpp
    tree/dynamic.cpp
    tree/snapshot.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/time_provider
    ydb/core/base
    ydb/core/protos
    ydb/core/resource_pools
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    yql/essentials/utils
    yt/yt/core
)

END()

RECURSE(
    service
)

RECURSE_FOR_TESTS(
    ut
)
