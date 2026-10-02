LIBRARY()

SRCS(
    cpu_load_actors.cpp
    pool_handlers_actors.cpp
    scheme_actors.cpp
)

PEERDIR(
    ydb/services/workload_manager/common
    ydb/services/workload_manager/tables

    ydb/core/tx/tx_proxy
)

YQL_LAST_ABI_VERSION()

END()
