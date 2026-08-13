LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
)

PEERDIR(
    library/cpp/retry
    library/cpp/threading/future
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/scheme_board
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/aclib
    ydb/library/actors/core
<<<<<<< HEAD
    ydb/services/metadata/secret
=======
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/services/scheme_secret
>>>>>>> 7afa21d8741 (AUTH_METHOD=IAM: implement ServiceAccountId permission check (#46107))
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_service
)
