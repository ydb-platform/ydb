LIBRARY()

SRCS(
    out.cpp
    proto_accessor.cpp
    table.cpp
)

GENERATE_ENUM_SERIALIZATION(table_enum.h)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common
    ydb/public/sdk/cpp/client/impl/ydb_internal/retry
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_params
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table/impl
    ydb/public/sdk/cpp/client/ydb_table/query_stats
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/public/sdk/cpp/client/ydb_value
)

END()
