LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/persqueue_public
)

END()
