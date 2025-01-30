LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    codecs.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/codecs
)

END()
