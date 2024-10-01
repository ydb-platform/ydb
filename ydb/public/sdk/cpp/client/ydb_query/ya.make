LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    client.h
    query.h
    stats.h
    tx.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/query
)

END()
