LIBRARY(ydb_sdk_core_access)

SRCS(
    ../ydb_sdk_core_access.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_types
)

END()
