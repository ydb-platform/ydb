LIBRARY()

SRCS(
    monitoring.cpp
)

GENERATE_ENUM_SERIALIZATION(monitoring.h)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
)

END()

