LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    ydb/library/yql/public/issue
)

END()
