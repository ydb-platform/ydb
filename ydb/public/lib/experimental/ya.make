LIBRARY()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_logstore.cpp
    ydb_object_storage.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/table
)

END()
