YQL_LIBRARY()

SRCS(
    classifier_metadata_tracker.cpp
    cpu_load_actors.cpp
    database_readiness_tracker.cpp
    pool_handlers_actors.cpp
    resource_pool_tracker.cpp
    workload_manager_state_actor.cpp
    scheme_actors.cpp
)

PEERDIR(
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables

    ydb/core/tx/tx_proxy
)

END()

RECURSE_FOR_TESTS(
    ut
)
