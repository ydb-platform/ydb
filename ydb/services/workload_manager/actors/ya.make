YQL_LIBRARY()

SRCS(
    cpu_load_actors.cpp
    pool_handlers_actors.cpp
    workload_manager_state_actor.cpp
    scheme_actors.cpp
)

PEERDIR(
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables

    ydb/core/tx/tx_proxy
)

END()
