LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    codecs.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/topic
)

END()
