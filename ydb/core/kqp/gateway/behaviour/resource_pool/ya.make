LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/cms/console
    ydb/core/kqp/gateway/actors
    ydb/core/protos
    ydb/core/resource_pools
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
