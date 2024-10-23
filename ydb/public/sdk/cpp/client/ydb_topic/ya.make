LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
)

END()
