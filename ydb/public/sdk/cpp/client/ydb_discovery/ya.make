LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    discovery.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
