LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_logstore.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_table
)

END()
