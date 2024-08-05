LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    codecs.h
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/topic/codecs
)

END()
