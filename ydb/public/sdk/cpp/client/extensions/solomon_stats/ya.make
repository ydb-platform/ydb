LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    pull_client.h
    pull_connector.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/extensions/solomon_stats
)

END()
