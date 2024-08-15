LIBRARY()

SRCS(
    operation.cpp
    out.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    ydb/public/sdk/cpp_v2/src/library/operation_id/
    ydb/public/sdk/cpp/client/ydb_types
)

END()
