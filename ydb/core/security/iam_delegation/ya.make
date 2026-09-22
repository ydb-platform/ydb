LIBRARY()

SRCS(
    cloud_resolver.cpp
    events.h
    iam_actor_base.h
    iam_delegated_token_service.cpp
    iam_delegation_service.cpp
    services.h
    settings.cpp
    system_token_service.cpp
)

PEERDIR(
    contrib/libs/googleapis-common-protos
    library/cpp/threading/future
    ydb/core/base
    ydb/core/protos
    ydb/core/util
    ydb/library/actors/async
    ydb/library/actors/core
    ydb/library/grpc/actor_client
    ydb/library/services
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/types/credentials
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
