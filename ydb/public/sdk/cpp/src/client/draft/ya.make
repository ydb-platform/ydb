LIBRARY()

SRCS(
    ydb_backup.cpp
    ydb_bridge.cpp
    ydb_dynamic_config.cpp
    ydb_udf.cpp
    ydb_replication.cpp
    ydb_scripting.cpp
    ydb_view.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_backup.h)
GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h)

PEERDIR(
    ydb/public/sdk/cpp/src/library/issue
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/value
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/impl/internal/make_request
    ydb/public/sdk/cpp/src/client/types/executor
)

END()
