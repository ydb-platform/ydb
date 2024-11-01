LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/library/grpc/client
    yql/essentials/public/issue
)

END()
