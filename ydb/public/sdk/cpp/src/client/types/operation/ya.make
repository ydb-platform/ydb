LIBRARY()

SRCS(
    operation.cpp
    out.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/types
)

END()
