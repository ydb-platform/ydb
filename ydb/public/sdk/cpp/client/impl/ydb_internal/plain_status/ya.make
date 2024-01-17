LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/grpc/client
    ydb/library/yql/public/issue
)

END()
