LIBRARY()

SRCS(
    iam_delegation.cpp
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/utils

    ydb/library/conclusion
    ydb/library/actors/async
    ydb/library/ycloud/impl
    ydb/public/sdk/cpp/src/client/iam

    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/secret
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
