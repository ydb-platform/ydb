YQL_LIBRARY()

SRCS(
    kqp_streaming_aggregation.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/threading/future
    ydb/core/kqp/runtime/common
    ydb/library/actors/core
    ydb/library/actors/helpers
    ydb/library/mkql_proto
    ydb/library/query_actor
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/client/proto
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
)

END()

RECURSE_FOR_TESTS(
    ut
)
