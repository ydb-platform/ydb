LIBRARY()

SRCS(
    query_actor.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/threading/future
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/library/yql/public/issue
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
