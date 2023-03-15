LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/grpc/client
    ydb/library/yql/public/issue
)

END()
