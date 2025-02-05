LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_common_client/impl
)

END()
