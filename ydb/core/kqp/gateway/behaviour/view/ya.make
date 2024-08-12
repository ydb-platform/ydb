LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/gateway/actors
    ydb/core/tx/tx_proxy
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
