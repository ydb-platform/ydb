LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/utils

    ydb/library/conclusion
    ydb/library/yql/dq/actors

    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/secret
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()
