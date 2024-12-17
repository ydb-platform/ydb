LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/services/metadata/initializer
    ydb/services/metadata/abstract
    ydb/services/metadata/secret
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/federated_query
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/gateway/behaviour/tablestore/operations
)

YQL_LAST_ABI_VERSION()

END()
