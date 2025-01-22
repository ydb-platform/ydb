LIBRARY()

SRCS(
    kqp_counters.cpp
    kqp_counters.h
    kqp_db_counters.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/tx_proxy
    ydb/core/sys_view/service
    ydb/library/yql/dq/actors/spilling
    yql/essentials/minikql
)

END()
