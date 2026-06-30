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
    ydb/library/yql/providers/common/token_accessor/client
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/iam_private
    ydb/services/metadata/secret
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut_service
)
