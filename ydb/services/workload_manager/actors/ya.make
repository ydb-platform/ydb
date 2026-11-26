YQL_LIBRARY()

SRCS(
    cpu_load_actors.cpp
    pool_handlers_actors.cpp
    resource_pool_tracker.cpp
    scheme_actors.cpp
    workload_manager_state_actor.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console/events
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/runtime/scheduler
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/resource_pools
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/table_creator
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables
)

END()

RECURSE_FOR_TESTS(
    ut
)
