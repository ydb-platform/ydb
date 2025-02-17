LIBRARY()

SRCS(
    cpu_load_actors.cpp
    pool_handlers_acors.cpp
    scheme_actors.cpp
)

PEERDIR(
    ydb/core/kqp/workload_service/common
    ydb/core/kqp/workload_service/tables

    ydb/core/tx/tx_proxy
)

YQL_LAST_ABI_VERSION()

END()
