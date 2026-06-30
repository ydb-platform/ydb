LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
    caching_iam_credentials_provider_service.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/base
    ydb/core/protos
    ydb/core/util
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/yql/providers/common/token_accessor/client
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/iam_private
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()
