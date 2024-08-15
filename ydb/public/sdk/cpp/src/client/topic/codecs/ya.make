LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    codecs.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic
)

END()
