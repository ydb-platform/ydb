LIBRARY()

SRCS(
    iam_actor_base.cpp
    iam_delegation_service.cpp
    settings.cpp
)

PEERDIR(
    contrib/libs/googleapis-common-protos
    ydb/core/base
    ydb/core/protos
    ydb/core/security/token_manager
    ydb/core/util
    ydb/library/actors/async
    ydb/library/actors/core
    ydb/library/grpc/actor_client
    ydb/library/services
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/public/api/protos
    yql/essentials/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
