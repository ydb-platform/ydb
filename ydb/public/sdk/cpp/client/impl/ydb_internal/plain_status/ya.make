LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/grpc/client
    ydb/public/api/protos
    yql/essentials/public/issue
)

END()
