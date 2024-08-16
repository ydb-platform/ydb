LIBRARY()

SRCS(
    ymq.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/string_utils/url
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
