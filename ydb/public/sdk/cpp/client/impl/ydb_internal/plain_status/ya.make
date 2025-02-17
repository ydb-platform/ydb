LIBRARY()

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
