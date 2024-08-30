LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    make.cpp
)

ADDINCL(
    ydb/public/sdk/cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
)

END()
