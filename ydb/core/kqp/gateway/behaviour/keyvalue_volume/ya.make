LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    manager.cpp
    s3_channels.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/provider
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/tx/scheme_cache
    ydb/core/tx/tiering/tier
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
    yql/essentials/core
    yql/essentials/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()
