LIBRARY()

# Avoid llvm-symbolizer cache thrashing for large line-table-only ASan binaries.
IF (SANITIZER_TYPE == "address" AND DEBUGINFO_LINES_ONLY == "yes")
    NO_DEBUG_INFO()
ENDIF()

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
    transaction.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/endpoints
    ydb/public/sdk/cpp/src/client/impl/internal/retry
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/impl/session
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/table/query_stats
    ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
