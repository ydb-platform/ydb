LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    settings.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/impl/ydb_internal/common
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()
