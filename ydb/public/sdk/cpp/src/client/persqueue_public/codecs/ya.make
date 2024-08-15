LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    codecs.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/codecs
)

END()
