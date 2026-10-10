YQL_LIBRARY()

SRCS(
    classifier_metadata_tracker.cpp
    cpu_load_actors.cpp
    database_readiness_tracker.cpp
    pool_handlers_actors.cpp
    resource_pool_tracker.cpp
    scheme_actors.cpp
    workload_manager_state_actor.cpp
)

PEERDIR(
    ydb/core/tx/tx_proxy
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables
)

END()

RECURSE_FOR_TESTS(
    ut
)
