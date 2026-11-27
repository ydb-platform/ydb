YQL_LIBRARY()

SRCS(
    kqp_counters.cpp
    kqp_counters.h
    kqp_db_counters.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/sys_view/service
    ydb/core/tx/tx_proxy
    ydb/library/yql/dq/actors/spilling/counters
    yql/essentials/minikql
)

END()
