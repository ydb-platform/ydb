LIBRARY()

OWNER(
    dcherednik
    g:kikimr
)

SRCS(
    base.h
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/string_utils/quote
    ydb/core/base
    ydb/core/grpc_streaming
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/resources
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
