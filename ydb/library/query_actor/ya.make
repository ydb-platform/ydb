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
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
