LIBRARY(ydb_sdk_core_access)

SRCS(
    ../ydb_sdk_core_access.cpp
    ../ydb_sdk_core_access.h
)

ADDINCL(
    ydb/public/sdk/cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/types
)
# gn: peerdir ydb/public/sdk/cpp/src/client/driver

END()
