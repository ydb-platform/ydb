LIBRARY()

SRCS(
    scheme.cpp
    analyze_actor.cpp
)

PEERDIR(
    ydb/core/tx/tx_proxy
    ydb/core/kqp/common
    ydb/core/kqp/provider
    ydb/library/yql/providers/common/gateway
    ydb/core/tx/schemeshard
    ydb/library/actors/core
    ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
