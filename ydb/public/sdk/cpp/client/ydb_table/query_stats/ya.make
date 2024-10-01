LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    stats.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/table/query_stats
)

END()
