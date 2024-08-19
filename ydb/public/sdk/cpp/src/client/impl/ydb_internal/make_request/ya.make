LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
)

END()
