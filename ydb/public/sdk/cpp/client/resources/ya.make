LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_ca.h
    ydb_resources.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/resources
)

END()
