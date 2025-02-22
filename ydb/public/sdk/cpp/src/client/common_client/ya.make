LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/common
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
