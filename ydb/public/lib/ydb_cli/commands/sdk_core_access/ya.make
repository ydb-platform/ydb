LIBRARY(ydb_sdk_core_access)

SRCS(
    ../ydb_sdk_core_access.cpp
)

ADDINCL(
    ydb/public/sdk/cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/types
)

END()
