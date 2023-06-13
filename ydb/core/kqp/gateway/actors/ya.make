LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    ydb/core/tx/tx_proxy
    ydb/core/kqp/common
    ydb/core/kqp/provider
    ydb/library/yql/providers/common/gateway
    ydb/core/tx/schemeshard
    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
