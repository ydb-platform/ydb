LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    initializer.cpp
    manager.cpp
    object.cpp
    queries.h
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    library/cpp/retry
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/provider
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/resource_pools
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/conclusion
    ydb/library/query_actor
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
