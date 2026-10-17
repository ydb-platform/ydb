YQL_LIBRARY()

SRCS(
    cpu_load_actors.cpp  # gn: cpu_load_actors
    pool_handlers_actors.cpp
    scheme_actors.cpp  # gn: scheme_actors
)

PEERDIR(
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables

    ydb/core/tx/tx_proxy
)

END()
