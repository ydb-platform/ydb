LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/plain_status
    ydb/public/sdk/cpp_v2/src/client/types
    ydb/public/sdk/cpp_v2/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

END()
