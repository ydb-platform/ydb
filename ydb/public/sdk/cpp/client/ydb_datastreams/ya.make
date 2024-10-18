LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    datastreams.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/datastreams
)

END()
