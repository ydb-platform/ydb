LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/kqp/federated_query
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/utils

    ydb/library/conclusion

    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/secret
)

YQL_LAST_ABI_VERSION()

END()
