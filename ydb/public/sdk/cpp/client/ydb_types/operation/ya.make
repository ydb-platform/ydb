LIBRARY()

SRCS(
    operation.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/ydb_types
)

END()
