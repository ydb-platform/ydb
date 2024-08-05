LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/common
    ydb/public/sdk/cpp_v2/src/client/types/credentials
)

END()
