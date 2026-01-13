LIBRARY()

SRCS(
    local_rpc.h
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services/base
    ydb/core/grpc_streaming
    ydb/library/actors/wilson
    ydb/library/wilson_ids
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/library/issue
)

YQL_LAST_ABI_VERSION()

END()
