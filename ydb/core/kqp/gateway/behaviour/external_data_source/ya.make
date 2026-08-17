LIBRARY()

SRCS(
    iam_delegation.cpp
    iam_delegation_ddl.cpp
    iam_delegation_ddl_bridge.cpp
    iam_delegation_ddl_runner.cpp
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/provider
    ydb/core/kqp/gateway/utils
    ydb/core/util

    ydb/library/conclusion
    ydb/library/actors/async
    ydb/library/yql/dq/actors
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/ycloud/impl

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
