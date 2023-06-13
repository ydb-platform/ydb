LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status
    ydb/public/sdk/cpp/client/ydb_types
    ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    ydb/library/yql/public/issue
)

END()
